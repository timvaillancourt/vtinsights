package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v62/github"
	"golang.org/x/oauth2"
)

const (
	owner = "vitessio"
	repo  = "vitess"
)

type maintainerTenure struct {
	addedAt   time.Time
	removedAt time.Time // zero value means still active
}

func (m maintainerTenure) activeAt(t time.Time) bool {
	if t.Before(m.addedAt) {
		return false
	}
	if !m.removedAt.IsZero() && t.After(m.removedAt) {
		return false
	}
	return true
}

// ReviewComment represents a maintainer's review comment with scoring and context.
type ReviewComment struct {
	ID          int64  `json:"id"`
	PRNumber    int    `json:"pr_number"`
	PRTitle     string `json:"pr_title,omitempty"`
	ReviewID    int64  `json:"review_id"`
	Author      string `json:"author"`
	Body        string `json:"body"`
	Path        string `json:"path"`
	Component   string `json:"component"`
	DiffHunk    string `json:"diff_hunk"`
	Line        int    `json:"line,omitempty"`
	InReplyToID int64  `json:"in_reply_to_id,omitempty"`
	CreatedAt   string `json:"created_at"`
	HTMLURL     string `json:"html_url"`
	Score       int    `json:"score"`
	ReviewState string `json:"review_state"`
}

func main() {
	minWords := flag.Int("min-words", 4, "Minimum word count to include a comment (filters low-value responses)")
	since := flag.String("since", "", "Only fetch comments since this date (YYYY-MM-DD). Default: 3 months ago")
	output := flag.String("output", "maintainer_comments.jsonl", "Output JSONL file path")
	token := flag.String("token", "", "GitHub token (or set GITHUB_TOKEN env var)")
	flag.Parse()

	ghToken := *token
	if ghToken == "" {
		ghToken = os.Getenv("GITHUB_TOKEN")
	}
	if ghToken == "" {
		log.Fatal("GitHub token required: set GITHUB_TOKEN env var or use -token flag")
	}

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: ghToken})
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	// Step 1: Determine time range
	var err error
	var sinceTime time.Time
	if *since != "" {
		sinceTime, err = time.Parse("2006-01-02", *since)
		if err != nil {
			log.Fatalf("Invalid -since date %q: %v", *since, err)
		}
	} else {
		sinceTime = time.Now().AddDate(0, -3, 0)
	}
	log.Printf("Fetching comments since %s", sinceTime.Format("2006-01-02"))

	// Step 2: Fetch and parse maintainers from MAINTAINERS.md
	maintainers, err := fetchMaintainers(ctx, client, sinceTime)
	if err != nil {
		log.Fatalf("Failed to fetch maintainers: %v", err)
	}
	for u, t := range maintainers {
		if t.removedAt.IsZero() {
			log.Printf("  %s: active since %s", u, t.addedAt.Format("2006-01-02"))
		} else {
			log.Printf("  %s: %s to %s (retired)", u, t.addedAt.Format("2006-01-02"), t.removedAt.Format("2006-01-02"))
		}
	}
	log.Printf("Found %d maintainers", len(maintainers))

	absOutput, _ := filepath.Abs(*output)

	// Truncate output file at start
	if err := os.WriteFile(absOutput, nil, 0644); err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}

	// Step 3: Collect, enrich, and write maintainer comments (one pass, JSONL)
	count, err := collectComments(ctx, client, maintainers, sinceTime, *minWords, absOutput)
	if err != nil {
		log.Printf("Error during collection: %v", err)
		log.Printf("Partial results (%d comments) saved to %s", count, absOutput)
	}

	log.Printf("Done. %d maintainer comments in %s", count, absOutput)
}

// collectComments pages through all repo-wide review comments, filters to
// maintainers, and appends each comment as a JSONL line immediately.
func collectComments(ctx context.Context, client *github.Client, maintainers map[string]maintainerTenure, since time.Time, minWords int, outputPath string) (int, error) {
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("opening output file: %w", err)
	}
	defer f.Close()

	total := 0
	reviewStateCache := make(map[string]string)
	prInfoCache := make(map[int]prInfo)

	opts := &github.PullRequestListCommentsOptions{
		Sort:      "created",
		Direction: "desc",
		Since:     since,
		ListOptions: github.ListOptions{
			PerPage: 100,
		},
	}

	page := 0
	for {
		opts.Page = page + 1
		comments, resp, err := client.PullRequests.ListComments(ctx, owner, repo, 0, opts)
		if err != nil {
			if waitForRateLimit(resp, err) {
				continue
			}
			if retryServerError(resp) {
				continue
			}
			return total, fmt.Errorf("listing comments (page %d): %w", opts.Page, err)
		}
		resetServerRetry()

		for _, c := range comments {
			if c.User == nil || c.User.Login == nil {
				continue
			}
			login := strings.ToLower(c.User.GetLogin())
			tenure, isMaintainer := maintainers[login]
			if !isMaintainer {
				continue
			}
			if !tenure.activeAt(c.GetCreatedAt().Time) {
				continue
			}

			// Skip if the maintainer is the PR author (self-reviews)
			prNumber := extractPRNumber(c.GetPullRequestURL())
			if prNumber == 0 {
				continue
			}
			pi, ok := prInfoCache[prNumber]
			if !ok {
				pi = fetchPRInfo(ctx, client, prNumber)
				prInfoCache[prNumber] = pi
			}
			if strings.EqualFold(login, pi.author) {
				continue
			}

			if len(strings.Fields(c.GetBody())) < minWords {
				continue
			}

			line := c.GetLine()
			if line == 0 {
				line = c.GetOriginalLine()
			}

			// Enrich: review state
			reviewID := c.GetPullRequestReviewID()
			reviewState := "COMMENTED"
			if reviewID != 0 {
				cacheKey := fmt.Sprintf("%d/%d", prNumber, reviewID)
				if cached, ok := reviewStateCache[cacheKey]; ok {
					reviewState = cached
				} else {
					reviewState = fetchReviewState(ctx, client, prNumber, reviewID)
					reviewStateCache[cacheKey] = reviewState
				}
			}

			// PR title (already cached from author lookup)
			prTitle := pi.title

			score := 1
			if strings.EqualFold(reviewState, "CHANGES_REQUESTED") {
				score = 2
			}

			rc := ReviewComment{
				ID:          c.GetID(),
				PRNumber:    prNumber,
				PRTitle:     prTitle,
				ReviewID:    reviewID,
				Author:      c.GetUser().GetLogin(),
				Body:        c.GetBody(),
				Path:        c.GetPath(),
				Component:   extractComponent(c.GetPath()),
				DiffHunk:    c.GetDiffHunk(),
				Line:        line,
				InReplyToID: derefInt64(c.InReplyTo),
				CreatedAt:   c.GetCreatedAt().Format(time.RFC3339),
				HTMLURL:     c.GetHTMLURL(),
				Score:       score,
				ReviewState: reviewState,
			}

			data, err := json.Marshal(rc)
			if err != nil {
				log.Printf("Warning: marshal error: %v", err)
				continue
			}
			fmt.Fprintf(f, "%s\n", data)
			total++
		}

		log.Printf("Page %d: %d comments, %d maintainer comments total", opts.Page, len(comments), total)

		if resp.NextPage == 0 {
			break
		}
		page = resp.NextPage - 1
	}

	return total, nil
}

func fetchReviewState(ctx context.Context, client *github.Client, prNumber int, reviewID int64) string {
	review, resp, err := client.PullRequests.GetReview(ctx, owner, repo, prNumber, reviewID)
	if err != nil {
		if waitForRateLimit(resp, err) {
			review, _, err = client.PullRequests.GetReview(ctx, owner, repo, prNumber, reviewID)
		}
		if err != nil {
			log.Printf("Warning: review %d for PR #%d: %v", reviewID, prNumber, err)
			return "COMMENTED"
		}
	}
	return review.GetState()
}

type prInfo struct {
	title  string
	author string
}

func fetchPRInfo(ctx context.Context, client *github.Client, prNumber int) prInfo {
	pr, resp, err := client.PullRequests.Get(ctx, owner, repo, prNumber)
	if err != nil {
		if waitForRateLimit(resp, err) {
			pr, _, err = client.PullRequests.Get(ctx, owner, repo, prNumber)
		}
		if err != nil {
			log.Printf("Warning: PR #%d: %v", prNumber, err)
			return prInfo{}
		}
	}
	return prInfo{
		title:  pr.GetTitle(),
		author: strings.ToLower(pr.GetUser().GetLogin()),
	}
}

// fetchMaintainers walks the commit history of MAINTAINERS.md to determine
// when each maintainer was added and removed. Returns a map of username -> tenure.
func fetchMaintainers(ctx context.Context, client *github.Client, since time.Time) (map[string]maintainerTenure, error) {
	re := regexp.MustCompile(`\[([a-zA-Z0-9_-]+)\]\(https://github\.com/[a-zA-Z0-9_-]+\)`)

	// Get commits touching MAINTAINERS.md within the time range
	opts := &github.CommitsListOptions{
		Path:        "MAINTAINERS.md",
		Since:       since,
		ListOptions: github.ListOptions{PerPage: 100},
	}

	type commitInfo struct {
		sha  string
		date time.Time
	}
	var commits []commitInfo

	for {
		page, resp, err := client.Repositories.ListCommits(ctx, owner, repo, opts)
		if err != nil {
			return nil, fmt.Errorf("listing commits for MAINTAINERS.md: %w", err)
		}
		for _, c := range page {
			date := c.GetCommit().GetCommitter().GetDate().Time
			commits = append(commits, commitInfo{sha: c.GetSHA(), date: date})
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	log.Printf("Found %d commits touching MAINTAINERS.md", len(commits))

	// Walk commits from oldest to newest, tracking additions and removals
	maintainers := make(map[string]maintainerTenure)
	prevActive := make(map[string]bool)

	for i := len(commits) - 1; i >= 0; i-- {
		c := commits[i]
		fileContent, _, _, err := client.Repositories.GetContents(ctx, owner, repo, "MAINTAINERS.md",
			&github.RepositoryContentGetOptions{Ref: c.sha})
		if err != nil {
			log.Printf("Warning: could not fetch MAINTAINERS.md at %s: %v", c.sha[:8], err)
			continue
		}
		content, err := fileContent.GetContent()
		if err != nil {
			continue
		}

		// Parse usernames from this version's active list
		current := make(map[string]bool)
		for _, line := range strings.Split(content, "\n") {
			if strings.HasPrefix(line, "## ") {
				break
			}
			if strings.HasPrefix(line, "* ") {
				matches := re.FindStringSubmatch(line)
				if len(matches) >= 2 {
					current[strings.ToLower(matches[1])] = true
				}
			}
		}

		// Detect additions: in current but not in previous
		for username := range current {
			if _, exists := maintainers[username]; !exists {
				maintainers[username] = maintainerTenure{addedAt: c.date}
			} else if !prevActive[username] {
				// Re-added after removal
				t := maintainers[username]
				t.removedAt = time.Time{}
				maintainers[username] = t
			}
		}

		// Detect removals: in previous but not in current
		for username := range prevActive {
			if !current[username] {
				t := maintainers[username]
				t.removedAt = c.date
				maintainers[username] = t
			}
		}

		prevActive = current
	}

	return maintainers, nil
}

var serverRetryCount int

// retryServerError retries on 5xx errors with exponential backoff.
// Returns true if the caller should retry.
func retryServerError(resp *github.Response) bool {
	if resp == nil || resp.StatusCode < 500 {
		return false
	}
	const maxRetries = 5
	if serverRetryCount >= maxRetries {
		serverRetryCount = 0
		return false
	}
	backoff := time.Duration(1<<serverRetryCount) * 5 * time.Second // 5s, 10s, 20s, 40s, 80s
	log.Printf("Server error %d, retry %d/%d in %s...", resp.StatusCode, serverRetryCount+1, maxRetries, backoff)
	time.Sleep(backoff)
	serverRetryCount++
	return true
}

// resetServerRetry resets the retry counter after a successful request.
func resetServerRetry() {
	serverRetryCount = 0
}

// waitForRateLimit checks if an error is a rate limit error and sleeps until
// the limit resets. Returns true if the caller should retry.
func waitForRateLimit(resp *github.Response, err error) bool {
	if resp == nil {
		return false
	}
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
		resetAt := resp.Rate.Reset.Time
		sleepDuration := time.Until(resetAt) + time.Second
		if sleepDuration > 0 && sleepDuration < 70*time.Minute {
			log.Printf("Rate limited. Waiting %s until %s ...", sleepDuration.Round(time.Second), resetAt.Format(time.RFC3339))
			time.Sleep(sleepDuration)
			return true
		}
	}
	return false
}

func extractPRNumber(url string) int {
	parts := strings.Split(url, "/")
	if len(parts) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(parts[len(parts)-1])
	return n
}

func derefInt64(p *int64) int64 {
	if p != nil {
		return *p
	}
	return 0
}

func extractComponent(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 3 && parts[0] == "go" && parts[1] == "vt" {
		return parts[2]
	}
	if len(parts) >= 3 && parts[0] == "go" && parts[1] == "cmd" {
		return parts[2]
	}
	if len(parts) >= 1 {
		return parts[0]
	}
	return "other"
}


package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/manifoldco/promptui"
)

type jobRow struct {
	id, name, queue string
	status          types.JobStatus
}

func main() {
	doBuild := flag.Bool("build", false, "GH manual-test image build from cwd jobs/<dir>")
	doLogs := flag.Bool("logs", false, "pick a running job and follow CloudWatch logs")
	doFollow := flag.Bool("follow", false, "with -logs, last 10m then new events")
	platform := flag.String("platform", "linux/arm64", "build platform: linux/arm64 or linux/amd64")
	marker := flag.String("marker", "", "job def name substring; default is `gh api user` login")
	flag.Parse()
	if *doBuild == *doLogs {
		fmt.Fprintf(os.Stderr, "use -build or -logs\n")
		flag.Usage()
		os.Exit(2)
	}
	if *doFollow && !*doLogs {
		log.Fatal("-follow requires -logs")
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatal(err)
	}
	client := batch.NewFromConfig(cfg)

	if *doBuild {
		if err := buildManualTest(ctx, client, *platform); err != nil {
			log.Fatal(err)
		}
		return
	}

	m := *marker
	if m == "" {
		m, err = ghLogin()
		if err != nil {
			log.Fatal(err)
		}
	}

	logsClient := cloudwatchlogs.NewFromConfig(cfg)
	jobs, err := collectJobs(ctx, client, m)
	if err != nil {
		log.Fatal(err)
	}
	if len(jobs) == 0 {
		log.Fatal("no running jobs")
	}

	labels := make([]string, len(jobs))
	for i, j := range jobs {
		labels[i] = fmt.Sprintf("%s  %s  %s  %s", j.status, j.name, j.id, j.queue)
	}
	i, _, err := (&promptui.Select{Label: "Job", Items: labels, Size: 15}).Run()
	if err != nil {
		log.Fatal(err)
	}
	if err := followLogs(ctx, client, logsClient, jobs[i].id, *doFollow); err != nil {
		log.Fatal(err)
	}
}

func collectJobs(ctx context.Context, client *batch.Client, marker string) ([]jobRow, error) {
	prefixes, err := defPrefixes(ctx, client, marker)
	if err != nil {
		return nil, err
	}
	queues, err := allQueues(ctx, client)
	if err != nil {
		return nil, err
	}

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		sem  = make(chan struct{}, 12)
		jobs []jobRow
	)
	for _, q := range queues {
		for _, p := range prefixes {
			wg.Add(1)
			sem <- struct{}{}
			go func(q, p string) {
				defer wg.Done()
				defer func() { <-sem }()
				found, err := listJobs(ctx, client, q, p)
				if err != nil {
					log.Printf("%s %s: %v", q, p, err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, j := range found {
					jobs = append(jobs, jobRow{
						id:     aws.ToString(j.JobId),
						name:   aws.ToString(j.JobName),
						queue:  q,
						status: j.Status,
					})
				}
			}(q, p)
		}
	}
	wg.Wait()
	return jobs, nil
}

func ghLogin() (string, error) {
	out, err := exec.Command("gh", "api", "user", "--jq", ".login").Output()
	if err != nil {
		return "", fmt.Errorf("pass -marker or log in with gh: %w", err)
	}
	login := strings.TrimSpace(string(out))
	if login == "" {
		return "", fmt.Errorf("pass -marker: gh returned an empty login")
	}
	return login, nil
}

func defPrefixes(ctx context.Context, client *batch.Client, marker string) ([]string, error) {
	seen := map[string]struct{}{}
	p := batch.NewDescribeJobDefinitionsPaginator(client, &batch.DescribeJobDefinitionsInput{
		Status: aws.String("ACTIVE"),
	})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, jd := range out.JobDefinitions {
			name := aws.ToString(jd.JobDefinitionName)
			i := strings.Index(name, marker)
			if i < 0 {
				continue
			}
			seen[name[:i+len(marker)]+"*"] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out, nil
}

func allQueues(ctx context.Context, client *batch.Client) ([]string, error) {
	var names []string
	p := batch.NewDescribeJobQueuesPaginator(client, &batch.DescribeJobQueuesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, q := range out.JobQueues {
			names = append(names, aws.ToString(q.JobQueueName))
		}
	}
	return names, nil
}

func listJobs(ctx context.Context, client *batch.Client, queue, prefix string) ([]types.JobSummary, error) {
	var jobs []types.JobSummary
	p := batch.NewListJobsPaginator(client, &batch.ListJobsInput{
		JobQueue: aws.String(queue),
		Filters: []types.KeyValuesPair{{
			Name:   aws.String("JOB_DEFINITION"),
			Values: []string{prefix},
		}},
	})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, j := range out.JobSummaryList {
			if j.Status == types.JobStatusRunning {
				jobs = append(jobs, j)
			}
		}
	}
	return jobs, nil
}

func followLogs(ctx context.Context, client *batch.Client, logsClient *cloudwatchlogs.Client, jobID string, followOnly bool) error {
	const since = 10 * time.Minute
	var token *string
	for {
		group, stream, running, err := logTarget(ctx, client, jobID)
		if err != nil {
			return err
		}
		if stream == "" {
			if !running {
				return fmt.Errorf("no log stream for %s", jobID)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		in := &cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(stream),
			NextToken:     token,
			StartFromHead: aws.Bool(true),
		}
		if token == nil && followOnly {
			in.StartTime = aws.Int64(time.Now().Add(-since).UnixMilli())
		}
		out, err := logsClient.GetLogEvents(ctx, in)
		if err != nil {
			if running && token == nil {
				time.Sleep(2 * time.Second)
				continue
			}
			return err
		}
		for _, e := range out.Events {
			fmt.Println(aws.ToString(e.Message))
		}
		next := out.NextForwardToken
		if aws.ToString(next) == aws.ToString(token) {
			if !running {
				return nil
			}
			time.Sleep(2 * time.Second)
			continue
		}
		token = next
	}
}

func logTarget(ctx context.Context, client *batch.Client, jobID string) (group, stream string, running bool, err error) {
	out, err := client.DescribeJobs(ctx, &batch.DescribeJobsInput{Jobs: []string{jobID}})
	if err != nil {
		return "", "", false, err
	}
	if len(out.Jobs) == 0 {
		return "", "", false, fmt.Errorf("job %s not found", jobID)
	}
	j := out.Jobs[0]
	running = j.Status == types.JobStatusRunning || j.Status == types.JobStatusStarting
	if j.Container == nil {
		return "/aws/batch/job", "", running, nil
	}
	group = "/aws/batch/job"
	if j.Container.LogConfiguration != nil {
		if g := j.Container.LogConfiguration.Options["awslogs-group"]; g != "" {
			group = g
		}
	}
	return group, aws.ToString(j.Container.LogStreamName), running, nil
}

func buildManualTest(ctx context.Context, client *batch.Client, platform string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	repoRoot, jobDir, ok := detectJobDir(cwd)
	if !ok {
		return fmt.Errorf("cwd is not inside jobs/<dir> with a Dockerfile")
	}
	branch, err := gitBranch(repoRoot)
	if err != nil {
		return err
	}
	jobDef, ecrRepo, err := prodBuildInputs(ctx, client, jobDir)
	if err != nil {
		return err
	}
	fmt.Printf("Building %s on %s %s (%s → %s)\n", jobDir, branch, platform, jobDef, ecrRepo)

	cmd := exec.Command("gh", "workflow", "run", "Build job image (manual test)",
		"--ref", branch,
		"-f", "job_dir="+jobDir,
		"-f", "ecr_repo="+ecrRepo,
		"-f", "job_definition="+jobDef,
		"-f", "platform="+platform,
	)
	cmd.Dir = repoRoot
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gh workflow run: %w", err)
	}

	time.Sleep(2 * time.Second)
	urlCmd := exec.Command("gh", "run", "list",
		"--workflow", "Build job image (manual test)",
		"--branch", branch,
		"--limit", "1",
		"--json", "url",
		"--jq", ".[0].url",
	)
	urlCmd.Dir = repoRoot
	if out, err := urlCmd.Output(); err == nil {
		fmt.Printf("GH build: %s\n", strings.TrimSpace(string(out)))
	} else {
		fmt.Println("GH build started")
	}
	return nil
}

func detectJobDir(cwd string) (repoRoot, jobDir string, ok bool) {
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			repoRoot = dir
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", "", false
		}
		dir = parent
	}
	rel, err := filepath.Rel(repoRoot, cwd)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return repoRoot, "", false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) < 2 || parts[0] != "jobs" {
		return repoRoot, "", false
	}
	jobDir = parts[1]
	if _, err := os.Stat(filepath.Join(repoRoot, "jobs", jobDir, "Dockerfile")); err != nil {
		return repoRoot, "", false
	}
	return repoRoot, jobDir, true
}

func gitBranch(repoRoot string) (string, error) {
	out, err := exec.Command("git", "-C", repoRoot, "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		return "", err
	}
	b := strings.TrimSpace(string(out))
	if b == "HEAD" {
		sha, err := exec.Command("git", "-C", repoRoot, "rev-parse", "HEAD").Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(sha)), nil
	}
	return b, nil
}

func prodBuildInputs(ctx context.Context, client *batch.Client, jobDir string) (jobDef, ecrRepo string, err error) {
	names := []string{
		"podscribe-jobs-" + jobDir,
		"podscribe-jobs-" + strings.ReplaceAll(jobDir, "_", "-"),
		"podscribe-jobs-" + strings.ReplaceAll(jobDir, "-", "_"),
	}
	seen := map[string]struct{}{}
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		var latest *types.JobDefinition
		p := batch.NewDescribeJobDefinitionsPaginator(client, &batch.DescribeJobDefinitionsInput{
			JobDefinitionName: aws.String(name),
			Status:            aws.String("ACTIVE"),
		})
		for p.HasMorePages() {
			out, err := p.NextPage(ctx)
			if err != nil {
				return "", "", err
			}
			for i := range out.JobDefinitions {
				d := &out.JobDefinitions[i]
				if latest == nil || aws.ToInt32(d.Revision) > aws.ToInt32(latest.Revision) {
					latest = d
				}
			}
		}
		if latest == nil {
			continue
		}
		image := ""
		if latest.ContainerProperties != nil {
			image = aws.ToString(latest.ContainerProperties.Image)
		}
		if image == "" {
			return "", "", fmt.Errorf("job definition %s has no image", name)
		}
		return name, ecrRepoFromImage(image), nil
	}
	return "", "", fmt.Errorf("no active job definition for %s (tried %s)", jobDir, strings.Join(names, ", "))
}

func ecrRepoFromImage(image string) string {
	s := image
	if i := strings.Index(s, ".amazonaws.com/"); i >= 0 {
		s = s[i+len(".amazonaws.com/"):]
	}
	if i := strings.Index(s, "@"); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndex(s, ":"); i >= 0 && !strings.Contains(s[i:], "/") {
		s = s[:i]
	}
	return s
}

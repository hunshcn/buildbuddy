package nix

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

type Provider struct {
	env environment.Env
}

func NewProvider(env environment.Env) *Provider {
	return &Provider{env: env}
}

func (p *Provider) New(ctx context.Context, props *platform.Properties, _ *repb.ScheduledTask, _ *rnpb.RunnerState, _ string) (container.CommandContainer, error) {
	return &NixCommandContainer{env: p.env, pkgs: append(props.NixPkgs, "bash")}, nil
}

type NixCommandContainer struct {
	env     environment.Env
	workDir string
	pkgs    []string
}

func (c *NixCommandContainer) IsolationType() string {
	return "nix"
}

func (c *NixCommandContainer) Run(ctx context.Context, command *repb.Command, workingDir string, creds oci.Credentials) *interfaces.CommandResult {
	return c.exec(ctx, command, workingDir, &interfaces.Stdio{})
}

func (c *NixCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	return false, nil
}

func (c *NixCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	return nil
}

func (c *NixCommandContainer) Create(ctx context.Context, workDir string) error {
	c.workDir = workDir
	return nil
}

func (c *NixCommandContainer) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	return c.exec(ctx, command, c.workDir, stdio)
}

func (c *NixCommandContainer) exec(ctx context.Context, command *repb.Command, workDir string, stdio *interfaces.Stdio) *interfaces.CommandResult {
	// An awful hack that sort of works for genrules could be:
	// command.Arguments[2] = strings.ReplaceAll(strings.ReplaceAll(command.Arguments[2], "; ", "; nix-shell -p "+strings.Join(c.pkgs, " ")+" --run '"), " > ", "' > ")
	// return c.env.GetCommandRunner().Run(ctx, command, workDir, nil, stdio)
	return nil
}

func (c *NixCommandContainer) Unpause(ctx context.Context) error {
	return nil
}

func (c *NixCommandContainer) Pause(ctx context.Context) error {
	return nil
}

func (c *NixCommandContainer) Remove(ctx context.Context) error {
	return nil
}

func (c *NixCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	return nil, nil
}

func (c *NixCommandContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, nil
}

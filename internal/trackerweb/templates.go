package trackerweb

const landingTemplate = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SavewebHQ</title></head>
<body><main><h1>SavewebHQ</h1><p>Contributor control plane.</p>
{{if .OAuthEnabled}}<p><a href="/auth/github/start">Sign in with GitHub</a></p>
{{else}}<p>GitHub login is not configured on this tracker.</p>{{end}}
</main></body></html>`

const portalTemplate = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SavewebHQ portal</title></head>
<body><main><h1>Contributor portal</h1>
<p>GitHub: {{.User.GitHubLogin}} · status: {{.User.Status}} · roles: {{range .Roles}}{{.}} {{else}}none{{end}}</p>
{{if .IsAdmin}}<p><a href="/admin/users">User administration</a> · <a href="/admin/projects">Project administration</a></p>{{end}}
<h2>Reusable machine setup token</h2>
{{if .MachineToken}}<p><code>{{.MachineToken}}</code></p>
<p>This token is shared by your machines. Keep it private. Resetting it immediately invalidates the old value.</p>
{{else}}<p>No machine token has been generated.</p>{{end}}
<form method="post" action="/portal/machine-token/reset">
<input type="hidden" name="csrf" value="{{.CSRF}}">
<button type="submit">{{if .MachineToken}}Reset{{else}}Generate{{end}} machine token</button>
</form>
<h2>Machines</h2><table><thead><tr><th>ID</th><th>Kind</th><th>Name</th><th>Status</th><th>Endpoint status</th><th>Last heartbeat</th></tr></thead><tbody>
{{range .Agents}}<tr><td><code>{{.ID}}</code></td><td>{{.Kind}}</td><td>{{.Name}}</td><td>{{.Status}}</td><td>{{.EndpointStatus}}</td><td>{{if .LastHeartbeatAt}}{{.LastHeartbeatAt}}{{else}}never{{end}}</td></tr>
{{else}}<tr><td colspan="6">No machines registered.</td></tr>{{end}}</tbody></table>
<form method="post" action="/logout"><input type="hidden" name="csrf" value="{{.CSRF}}"><button type="submit">Sign out</button></form>
</main></body></html>`

const adminUsersTemplate = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SavewebHQ users</title></head>
<body><main><h1>User administration</h1><p><a href="/portal">Back to portal</a> · <a href="/admin/projects">Project administration</a></p>
{{range .Users}}<section><h2>{{if .GitHubLogin}}{{.GitHubLogin}}{{else}}{{.ID}}{{end}}</h2>
<p>ID: <code>{{.ID}}</code>{{if .GitHubUserID}} · GitHub ID: {{.GitHubUserID}}{{end}}</p>
<form method="post" action="/admin/users/{{.ID}}/access">
<input type="hidden" name="csrf" value="{{$.CSRF}}">
<label>Status <select name="status">
<option value="pending" {{if eq .Status "pending"}}selected{{end}}>pending</option>
<option value="active" {{if eq .Status "active"}}selected{{end}}>active</option>
<option value="suspended" {{if eq .Status "suspended"}}selected{{end}}>suspended</option>
</select></label>
<label><input type="checkbox" name="role_worker" {{if index .Roles "worker"}}checked{{end}}>worker</label>
<label><input type="checkbox" name="role_shard_owner" {{if index .Roles "shard_owner"}}checked{{end}}>shard owner</label>
<label><input type="checkbox" name="role_admin" {{if index .Roles "admin"}}checked{{end}}>admin</label>
<label>Reason <input name="reason" maxlength="1000" required></label>
<button type="submit">Apply access</button></form></section>{{end}}
</main></body></html>`

const adminProjectsTemplate = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SavewebHQ projects</title></head>
<body><main><h1>Project administration</h1>
<p><a href="/portal">Back to portal</a> · <a href="/admin/users">User administration</a></p>

<h2>Projects</h2>
<table><thead><tr><th>ID</th><th>Status</th></tr></thead><tbody>
{{range .Projects}}<tr><td><code>{{.ID}}</code></td><td>{{.Status}}</td></tr>
{{else}}<tr><td colspan="2">No projects registered.</td></tr>{{end}}
</tbody></table>

<h2>Queue shards</h2>
<table><thead><tr><th>Project / shard</th><th>Status</th><th>Owner</th><th>Generation</th><th>Owner lease</th><th>Source</th><th>Checkpoint</th><th>Error</th><th>Lifecycle</th></tr></thead><tbody>
{{range .Shards}}<tr>
<td><code>{{.ProjectID}}/{{.ID}}</code></td><td>{{.Status}}</td><td><code>{{.OwnerAgentID}}</code></td>
<td>{{.Generation}}</td><td>{{.OwnerLeaseExpiresAt}}</td>
<td>{{if .SourceURI}}<code>{{.SourceURI}}</code><br>ETag: <code>{{.SourceETag}}</code>{{else}}none{{end}}</td>
<td>{{if .CheckpointURI}}seq {{.CheckpointSequence}} / generation {{.CheckpointGeneration}}<br><code>{{.CheckpointURI}}</code>{{else}}none{{end}}{{if .CheckpointUploadID}}<br>upload: <code>{{.CheckpointUploadID}}</code>{{end}}</td>
<td>{{if .LoadErrorCode}}load: <code>{{.LoadErrorCode}}</code>{{end}}{{if .RecoveryErrorCode}} recovery: <code>{{.RecoveryErrorCode}}</code>{{end}}</td>
<td>{{if eq .Status "active"}}<form method="post" action="/admin/shards/transition">
<input type="hidden" name="csrf" value="{{$.CSRF}}"><input type="hidden" name="project_id" value="{{.ProjectID}}"><input type="hidden" name="shard_id" value="{{.ID}}"><input type="hidden" name="expected_generation" value="{{.Generation}}"><input type="hidden" name="target_status" value="draining">
<label>Reason <input name="reason" maxlength="1000" required></label><button type="submit">Drain</button></form>
{{else if eq .Status "draining"}}<form method="post" action="/admin/shards/transition">
<input type="hidden" name="csrf" value="{{$.CSRF}}"><input type="hidden" name="project_id" value="{{.ProjectID}}"><input type="hidden" name="shard_id" value="{{.ID}}"><input type="hidden" name="expected_generation" value="{{.Generation}}">
<label>Reason <input name="reason" maxlength="1000" required></label><button name="target_status" value="active" type="submit">Resume claims</button>{{if .CheckpointURI}}<button name="target_status" value="paused" type="submit">Pause</button>{{else}} Published checkpoint required before pause.{{end}}</form>
{{else if eq .Status "paused"}}Recover below with a higher generation.{{else}}No direct transition.{{end}}</td>
</tr>{{else}}<tr><td colspan="9">No queue shards registered.</td></tr>{{end}}
</tbody></table>

<p>Drain stops new claims after the owner's next heartbeat while allowing existing attempts and checkpoints. Pause requires a published checkpoint, clears the owner lease, and can only follow draining.</p>

<h2>Job Receivers</h2>
<table><thead><tr><th>Project / receiver</th><th>Status</th><th>Format</th><th>Sink</th></tr></thead><tbody>
{{range .Receivers}}<tr><td><code>{{.ProjectID}}/{{.ID}}</code></td><td>{{.Status}}</td><td>{{.Format}}</td><td><code>{{.SinkURI}}</code></td></tr>
{{else}}<tr><td colspan="4">No Job Receivers registered.</td></tr>{{end}}
</tbody></table>

<h2>Create or update Project</h2>
<form method="post" action="/admin/projects">
<input type="hidden" name="csrf" value="{{.CSRF}}">
<label>Project ID <input name="project_id" maxlength="255" required></label>
<label>Status <select name="status"><option value="active">active</option><option value="draining">draining</option><option value="archived">archived</option></select></label>
<label>Reason <input name="reason" maxlength="1000" required></label>
<button type="submit">Apply Project</button>
</form>

<h2>Attach source shard or recover checkpoint</h2>
<p>Loading requires all source fields. Recovering requires a newer generation, an existing published checkpoint, and blank source fields.</p>
<form method="post" action="/admin/shards">
<input type="hidden" name="csrf" value="{{.CSRF}}">
<label>Project ID <input name="project_id" maxlength="255" required></label>
<label>Shard ID <input name="shard_id" maxlength="255" required></label>
<label>Owner agent <input name="owner_agent_id" list="shard-agents" maxlength="255" required></label>
<datalist id="shard-agents">{{range .ShardAgents}}<option value="{{.ID}}">{{.Name}} / {{.Status}} / {{.EndpointStatus}}</option>{{end}}</datalist>
<label>Status <select name="status"><option value="loading">loading source</option><option value="recovering">recovering checkpoint</option></select></label>
<label>Generation <input name="generation" type="number" min="1" step="1" required></label>
<label>Source URI <input name="source_uri" maxlength="4096" placeholder="s3://bucket/key"></label>
<label>Source format <select name="source_format"><option value="jobs-jsonl-zstd-v1">jobs-jsonl-zstd-v1</option><option value="">none (recovery)</option></select></label>
<label>Source ETag <input name="source_etag" maxlength="512"></label>
<label>Reason <input name="reason" maxlength="1000" required></label>
<button type="submit">Attach or recover Shard</button>
</form>

<h2>Create, update, or remove Job Receiver</h2>
<form method="post" action="/admin/receivers">
<input type="hidden" name="csrf" value="{{.CSRF}}">
<label>Project ID <input name="project_id" maxlength="255" required></label>
<label>Receiver ID <input name="receiver_id" maxlength="255" required></label>
<label>Status <select name="status"><option value="active">active</option><option value="removed">removed</option></select></label>
<label>Immutable sink prefix <input name="sink_uri" maxlength="4096" placeholder="s3://bucket/prefix" required></label>
<label>Reason <input name="reason" maxlength="1000" required></label>
<button type="submit">Apply Job Receiver</button>
</form>

<h2>Recent audit log</h2>
<table><thead><tr><th>Time</th><th>Actor</th><th>Action</th><th>Target</th><th>Reason</th></tr></thead><tbody>
{{range .Audit}}<tr><td>{{.CreatedAt}}</td><td><code>{{.ActorID}}</code></td><td>{{.Action}}</td><td><code>{{.TargetID}}</code></td><td>{{.Reason}}</td></tr>
{{else}}<tr><td colspan="5">No audit events.</td></tr>{{end}}
</tbody></table>
</main></body></html>`

const errorTemplate = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SavewebHQ error</title></head>
<body><main><h1>Request failed</h1><p>{{.Message}}</p><p><a href="/">Return</a></p></main></body></html>`

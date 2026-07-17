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
{{if .IsAdmin}}<p><a href="/admin/users">User administration</a></p>{{end}}
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
<body><main><h1>User administration</h1><p><a href="/portal">Back to portal</a></p>
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

const errorTemplate = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>SavewebHQ error</title></head>
<body><main><h1>Request failed</h1><p>{{.Message}}</p><p><a href="/">Return</a></p></main></body></html>`

# Security model

cdispatch operates a trusted user's resource pool. Task commands and explicitly
configured plugins execute code with the participating account's permissions.
This is not a multi-tenant sandbox. Shared steps constrain resources and lifetime,
not access between mutually untrusted users of the same Unix account.

Keep runtime state, site config, bot tokens and SSH credentials outside Git.
Delegated SSH must use administrator-controlled forced commands and fixed state
roots. Restrict access to the spool and config to authorized users.

Report suspected vulnerabilities privately to egr.vrn@gmail.com with a minimal
sanitized reproduction. Do not post secrets or destructive cluster examples.

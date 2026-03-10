# Web GUI and Policies

## Enable

Set:

```json
{
  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "authDbPath": "/data/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret"
}
```

## Core routes

- `/` home/map
- `/login`, `/logout`
- `/forgot-password`
- `/reset-password?token=...`
- `/request-account`
- `/change-password`
- `/fast-login?token=...`
- `/station/<uuid>` station browser + chart + download
- `/public/station/<uuid>` public station dashboard
- `/admin` admin console
- `/admin/dashboard` admin-only network dashboard
- `/anomalies` anomaly log and silence actions

## Access policies

Per station policy:

- `open`: no authentication for data download
- `account`: authenticated users
- `restricted`: only explicitly assigned users
- admins always allowed

Server-side permission checks are enforced for station browsing and downloads.

## User lifecycle and emails

With SMTP enabled:

- registration request confirmation email
- welcome email when admin creates user
- approval email when request is approved
- password-reset email from forgot-password flow
- anomaly warning emails to admins and station-related users

SMTP fallback behavior:

- if `smtpHost` is missing, emails are skipped
- if `smtpPort`, `smtpUser`, and `smtpPass` are omitted, the app uses unauthenticated SMTP on port `25`

## Password management

- admin can force user password change
- forced users are redirected to `/change-password` at login
- users can request a password reset from `/forgot-password`
- reset links are delivered by email and open `/reset-password?token=...`
- reset flow requires a valid `baseUrl` and working SMTP configuration

## Anomaly log and silencing

- anomalies are persisted in auth SQLite DB
- admins see full log
- non-admin users see only stations they can access
- users can silence specific anomalies for 1..24 hours

## Branding and logos

- `webAppLogo` sets global app logo
- station users can upload per-station logo from station page
- public station page shows app logo + station logo together
- public station dashboard trend charts update in-place without full page reload
- public station dashboard trend window can be selected as:
  - last minute
  - 10 minutes
  - hour
  - 3 hours
  - 6 hours
  - 12 hours
  - 24 hours
  - 72 hours
  - one week
- public station dashboard remembers selected trend window in a browser cookie
- station dashboard also remembers selected trend interval in a browser cookie
- when a public dashboard trend window is changed, trend chart x-axis is recomputed to match the selected time window

## Admin dashboard behavior

Admin dashboard groups fields by domain (Atmosphere/Wind/Rain/AQ/etc.) and highlights missing sensor values.

Connectivity alarms are based on dynamic threshold:

- station marked failing when latest data age > `2 * usual update interval`

## Security recommendations

- set strong `webSessionSecret`
- change default admin credentials
- run behind TLS reverse proxy
- keep `authDbPath` on secure persistent volume

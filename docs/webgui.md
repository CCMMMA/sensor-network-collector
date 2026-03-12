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
- `/station/<uuid>/chart-settings` station trend-chart Y-axis settings
- `/station/<uuid>/chart-settings/export`
- `/station/<uuid>/chart-settings/import`
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

## Chart control rights

Chart setup rights are separate from download/browse rights.

- admins can edit trend-chart settings for every station
- non-admin users can edit a station chart setup only if they have explicit chart-control rights for that station
- chart-control rights are assigned from `/admin`

This is used for the Public Station Dashboard trend charts only.

## Station browser chart and table preferences

The authenticated station browser page `/station/<uuid>` has its own user-side browsing preferences.

Chart section:

- available parameters in the center
- selected left-axis parameters on the left
- selected right-axis parameters on the right
- each selected parameter can define:
  - chart type (`line` or `bar`)
  - color
  - `y_min`
  - `y_max`
  - `y_step`
- each selected parameter has an `Auto range` action that computes `y_min`, `y_max`, and `y_step` from the parameter time series in the currently selected trend window
- chart updates automatically when parameters are moved or visualization options are changed

Table section:

- rows per page can be selected as:
  - `50`
  - `100`
  - `250`
  - `Trend window`
- `Trend window` shows all rows in the current trend window
- each table header embeds its own checkbox for show/hide control
- when a column is hidden, its header collapses to checkbox width and the full title remains available as tooltip
- the table section includes a `Statistics` area for visible numeric parameters in the current trend window
- each statistics card shows:
  - minimum and the timestamp when it occurred
  - maximum and the timestamp when it occurred
  - average
  - standard deviation

Persistence:

- station browser preferences are persisted per station
- the same preference object stores:
  - chart selection and left/right assignment
  - chart type
  - chart color
  - chart `y_min`
  - chart `y_max`
  - chart `y_step`
  - table page size
  - visible columns

Import/export:

- station browser preferences can be exported/imported as JSON from the chart section
- the JSON file includes both chart preferences and table preferences

## Public station chart axis settings

The Public Station Dashboard supports per-station Y-axis configuration for each trend chart.

Available settings per chart:

- `y_min`
- `y_max`
- `y_step`

Behavior:

- when all three values are empty, the dashboard computes a best-fit automatic axis range and interval
- when one or more values are set, the saved values override the automatic Y-axis calculation for that station/chart
- `y_step` controls the chart tick interval on the Y-axis
- saved settings are read from the auth SQLite database and applied to the public dashboard without requiring configuration-file changes

Management flow:

- open `/station/<uuid>/chart-settings`
- edit values for one or more charts
- leave fields empty to keep automatic sizing
- save changes

The page also shows:

- current effective Y-axis range used by the dashboard
- current effective Y-axis step
- last update timestamp and user who changed the setting

## Import and export of chart settings

Each station chart setup can be exported/imported as JSON.

Routes:

- export: `/station/<uuid>/chart-settings/export`
- import: `/station/<uuid>/chart-settings/import`

Use cases:

- copy the same chart policy between environments
- back up a station dashboard setup
- version-control station chart settings outside the SQLite database

JSON payload structure:

```json
{
  "station_uuid": "it.uniparthenope.meteo.ws1",
  "exported_at": "2026-03-12T12:00:00Z",
  "series": {
    "temperature": {
      "label": "Temperature Trend",
      "unit": "C",
      "y_min": 0,
      "y_max": 40,
      "y_step": 5
    },
    "humidity": {
      "label": "Humidity Trend",
      "unit": "%",
      "y_min": 0,
      "y_max": 100,
      "y_step": 10
    }
  }
}
```

Import rules:

- the file must contain a `series` object
- unknown chart keys are ignored
- `y_step` must be positive
- if both `y_min` and `y_max` are provided, `y_max` must be greater than `y_min`
- if `station_uuid` is present in the JSON and differs from the current station, the web UI prompts the user for confirmation before importing it into the current station
- the server accepts a cross-station import only when that explicit confirmation flag is sent from the form

This allows reusing the same chart setup across stations while still protecting against accidental imports into the wrong station.

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
- existing chart instances are reused and updated with fresh points and axes instead of being recreated on each polling cycle
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
- when the plotted range crosses a day boundary, the x-axis prints the full date at the tick where the day changes
- public station dashboard trend chart Y-axis settings are loaded per station from the auth SQLite DB
- Y-axis step size is applied directly to chart ticks when configured
- double-clicking a trend chart toggles a focused full-screen view for that chart
- double-clicking again restores the normal multi-chart layout
- pressing `Esc` also exits the focused chart view
- the focused chart state can be opened directly with query string `focus=<chart_key>`
- in focused mode the chart title shows the parameter name followed by the selected trend window
- in focused mode the top of the chart view shows:
  - web app logo
  - station name centered, without UUID
  - station logo
- when a chart is focused, the page shows compact numeric widgets horizontally below the chart for the selected trend window:
  - current
  - min
  - max
- the focused bottom stats bar is sized to keep current, min, and max visible within the viewport
- min and max widgets also show the date and time when those values occurred
- multi-series charts display one set of min/max/current values per sub-series

## Admin dashboard behavior

Admin dashboard groups fields by domain (Atmosphere/Wind/Rain/AQ/etc.) and highlights missing sensor values.

Connectivity alarms are based on dynamic threshold:

- station marked failing when latest data age > `2 * usual update interval`

## SQLite auth DB contents

The auth database now stores:

- users
- account requests
- station download policies
- per-user restricted station access
- per-user station chart-control rights
- login and password-reset tokens
- anomalies and anomaly silence windows
- station logos
- per-station public chart settings

## Security recommendations

- set strong `webSessionSecret`
- change default admin credentials
- run behind TLS reverse proxy
- keep `authDbPath` on secure persistent volume

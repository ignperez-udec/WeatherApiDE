#!/bin/sh
set -e

echo "Waiting for Metabase..."

until [ "$(curl -s $METABASE_URL/api/health | jq -r .status)" = "ok" ]; do
    echo "Metabase not ready..."
    sleep 5
done

echo "Metabase is healthy"
sleep 30

########################################
# 1. LOGIN
########################################
echo "Getting session token..."

SESSION=$(curl -v \
  -X POST \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$METABASE_USER\", \"password\":\"$METABASE_PASSWORD\"}" \
  $METABASE_URL/api/session | jq -r '.id')

########################################
# 2. CREATE ADMIN IF LOGIN FAILS
########################################
if [ "$SESSION" = "null" ] || [ -z "$SESSION" ]; then
    echo "Admin user not found. Creating admin..."

    curl -v \
      -X POST \
      -H "Content-Type: application/json" \
      -d "{
        \"token\": null,
        \"admin_ser\": {
          \"first_name\": \"Admin\",
          \"last_name\": \"User\",
          \"email\": \"$METABASE_USER\",
          \"password\": \"$METABASE_PASSWORD\"
        },
        \"prefs\": {
          \"site_locale\": \"en\",
          \"site_name\": \"Weather Dashboard\"
        },
        \"is_onboarding\": false,
        \"sso_enabled\": false,
        \"tracking_enabled\": false
      }" \
      $METABASE_URL/api/setup

    echo "Admin created"

    SESSION=$(curl -v \
      -X POST \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"$METABASE_USER\", \"password\":\"$METABASE_PASSWORD\"}" \
      $METABASE_URL/api/session | jq -r '.id')
else
    echo "Logged in successfully"
fi

echo "SESSION TOKEN: $SESSION"

########################################
# 3. CREATE DATABASE CONNECTION
########################################
echo "Creating PostgreSQL connection..."

curl -v \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION" \
  -d "{
    \"name\": \"WeatherDB\",
    \"engine\": \"postgres\",
    \"details\": {
      \"host\": \"$DB_HOST\",
      \"port\": $DB_PORT,
      \"dbname\": \"$DB_NAME\",
      \"user\": \"$DB_USER\",
      \"password\": \"$DB_PASSWORD\"
    }
  }" \
  $METABASE_URL/api/database > /dev/null

echo "Setup completed"
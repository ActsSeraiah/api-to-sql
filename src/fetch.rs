use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

/// Fetches JSON data from a REST API endpoint and saves it to a file.
/// Makes an HTTP GET request with a User-Agent header to comply with API requirements.
/// Optionally includes a Bearer token in the Authorization header for authenticated APIs.
/// The response is parsed as JSON and written to the output file in pretty-printed format.
///
/// # Arguments
/// * `url` - The API endpoint URL to fetch data from
/// * `bearer_token` - Optional Bearer token for API authentication
/// * `out` - Path to the output file where JSON will be saved
///
/// # Returns
/// Returns `Ok(())` on success, or an error if the request fails, returns non-200 status,
/// contains invalid JSON, or file writing fails.
pub async fn fetch_to_file(url: &str, bearer_token: Option<&str>, out: &PathBuf) -> Result<()> {
    let client = Client::new();
    let mut request = client
        .get(url)
        .header("User-Agent", "api_to_sql/0.1.0 (test@example.com)");

    if let Some(token) = bearer_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    let json: Value = request
        .send()
        .await
        .context("request failed")?
        .error_for_status()
        .context("request returned non-success status")?
        .json()
        .await
        .context("response was not valid JSON")?;

    fs::write(out, serde_json::to_string_pretty(&json)?)
        .with_context(|| format!("failed to write {}", out.display()))?;

    println!("Wrote API response JSON to {}", out.display());
    Ok(())
}
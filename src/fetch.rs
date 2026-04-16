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
/// * `x_api_key` - Optional API key value sent as x-api-key header
/// * `out` - Path to the output file where JSON will be saved
///
/// # Returns
/// Returns `Ok(())` on success, or an error if the request fails, returns non-200 status,
/// contains invalid JSON, or file writing fails.
pub async fn fetch_to_file(
    url: &str,
    bearer_token: Option<&str>,
    x_api_key: Option<&str>,
    out: &PathBuf,
) -> Result<()> {
    let client = Client::new();
    let mut request = client
        .get(url)
        .header("User-Agent", "api_to_sql/1.0.1 (test@example.com)");

    if let Some(token) = bearer_token {
        // Check if user accidentally included "Bearer " prefix
        if token.starts_with("Bearer ") {
            eprintln!("Warning: Bearer token appears to include 'Bearer ' prefix. \
                      The tool automatically adds this prefix, so please provide only the token value. \
                      Proceeding with token as provided, but consider removing the 'Bearer ' prefix.");
        }
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    if let Some(api_key) = x_api_key {
        request = request.header("x-api-key", api_key);
    }

    let response = request
        .send()
        .await
        .context("Failed to send HTTP request - check your internet connection and URL")?;

    // Check if the response is successful
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_else(|_| "No error details available".to_string());

        let error_message = format!(
            "API request failed with status {}: {}\n\
Please check:\n\
- Your API endpoint URL is correct\n\
- Your bearer token (if required) is valid\n\
- Your x-api-key (if required) is valid\n\
- The API is currently available\n\
- You have the necessary permissions",
            status, error_text
        );
        anyhow::bail!(error_message);
    }

    let json: Value = response
        .json()
        .await
        .context("Response was not valid JSON - the API may be returning an error page or unexpected format")?;

    fs::write(out, serde_json::to_string_pretty(&json)?)
        .with_context(|| format!("failed to write {}", out.display()))?;

    println!("Wrote API response JSON to {}", out.display());
    Ok(())
}
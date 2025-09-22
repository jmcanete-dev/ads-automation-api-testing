import requests
import logging
import pandas as pd

# Facebook API setup
FACEBOOK_API_VERSION = "v23.0"
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

# Replace with actual values
ad_account_id = "2442304332606254"
access_token = "EAAVh7S68SHwBO0YKocXkHyh0LnWZCN4KO00tU69lGhPXANOpZCPpaxB8CB2GhRyWlY1ZCbdUrPXbBEz56J8fGksTvHRo9O3zaj4sWdIpW7JoHxe9HbH0AJ9rnLTeTgyPis3fsOvlHMdkjxQZCJRWNTjvEkOUXZAj0cMggKGjaoLcHNciIZAZCmoRVc8L3GFfdJm"

def fetch_facebook_data(url, access_token):
    """Fetch data from Facebook API with error handling."""
    try:
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logging.error(f"Facebook API Error: {data['error']}")
            return None

        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Facebook API: {e}")
        return None

def process_facebook_data(data):
    """Flatten JSON data into a structured table format."""
    records = []

    for campaign in data:
        campaign_id = campaign.get("id", "")
        campaign_name = campaign.get("name", "")
        campaign_status = campaign.get("status", "")

        # Extract insights if available
        insights_data = campaign.get("insights", {}).get("data", [])
        campaign_insights = {}
        if insights_data:
            for insight in insights_data:
                if "cost_per_action_type" in insight:
                    for action in insight["cost_per_action_type"]:
                        campaign_insights[f"campaign_cost_{action['action_type']}"] = action["value"]

        # Extract adsets
        adsets = campaign.get("adsets", {}).get("data", [])
        if adsets:
            for adset in adsets:
                adset_id = adset.get("id", "")
                adset_name = adset.get("name", "")
                adset_status = adset.get("status", "")

                # Extract adset insights if available
                adset_insights_data = adset.get("insights", {}).get("data", [])
                adset_insights = {}
                if adset_insights_data:
                    for insight in adset_insights_data:
                        if "cost_per_action_type" in insight:
                            for action in insight["cost_per_action_type"]:
                                adset_insights[f"adset_cost_{action['action_type']}"] = action["value"]

                # Append structured data with both campaign and adset data
                records.append({
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "campaign_status": campaign_status,
                    **campaign_insights,  # Add campaign insights dynamically
                    "adset_id": adset_id,
                    "adset_name": adset_name,
                    "adset_status": adset_status,
                    **adset_insights,  # Add adset insights dynamically
                })
        else:
            # If no adsets, store only campaign data
            records.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "campaign_status": campaign_status,
                **campaign_insights,  # Add campaign insights dynamically
                "adset_id": None,
                "adset_name": None,
                "adset_status": None,
            })

    return records

# Construct the API URL
url = f"{FACEBOOK_GRAPH_URL}/act_{ad_account_id}/campaigns?fields=id,name,status,insights{{cost_per_action_type}},adsets{{id,name,status,insights{{cost_per_action_type}}}}"

all_records = []  # Store all fetched data

while url:
    response_data = fetch_facebook_data(url, access_token)
    if not response_data:
        break  # Exit loop if error occurs

    # Extract "data" field from response
    fetched_data = response_data.get("data", [])
    all_records.extend(process_facebook_data(fetched_data))

    # Get next page URL if available
    url = response_data.get("paging", {}).get("next")

# Convert to Pandas DataFrame
df = pd.DataFrame(all_records)

# Export to CSV
df.to_csv("facebook_ads_data.csv", index=False, encoding="utf-8")

print("✅ Data successfully exported to facebook_ads_data.csv")

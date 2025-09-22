import requests

def fetch_campaigns_with_insights(ad_account_id, access_token):
    """
    Fetch all campaigns, ad sets, ads, and insights by handling pagination.
    """
    base_url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/campaigns"
    params = {
        "fields": "id,name,status,objective,daily_budget,bid_strategy,"
                  "insights{cpp,cpm,spend,impressions},"
                  "adsets{id,name,status,insights{cpp,cpm,spend,impressions},"
                  "ads{id,name,insights{cpp,cpm,spend,impressions}}}",
        "access_token": access_token,
        "limit": 25  # Limit the number of results per request (can be adjusted as needed)
    }
    
    all_campaign_data = {}
    while True:
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            return {"error": "Failed to fetch data", "details": response.text}
        
        data = response.json()
        raw_data = data.get("data", [])
        
        # Process the current page of campaigns
        for campaign in raw_data:
            campaign_id = campaign.get("id")
            campaign_name = campaign.get("name")
            all_campaign_data[campaign_id] = {
                "id": campaign_id,
                "campaign_name": campaign_name,  # Prepend campaign name
                "status": campaign.get("status"),
                "objective": campaign.get("objective"),
                "daily_budget": campaign.get("daily_budget"),
                "bid_strategy": campaign.get("bid_strategy"),
                "campaign_insights": extract_insights(campaign.get("insights")),
                "adsets": {}
            }

            # Process adsets if available
            for adset in campaign.get("adsets", {}).get("data", []):
                adset_id = adset.get("id")
                adset_name = adset.get("name")
                all_campaign_data[campaign_id]["adsets"][adset_id] = {
                    "id": adset_id,
                    "adset_name": adset_name,  # Prepend adset name
                    "status": adset.get("status"),
                    "adset_insights": extract_insights(adset.get("insights")),
                    "ads": {}
                }

                # Process ads if available
                for ad in adset.get("ads", {}).get("data", []):
                    ad_id = ad.get("id")
                    ad_name = ad.get("name")
                    all_campaign_data[campaign_id]["adsets"][adset_id]["ads"][ad_id] = {
                        "id": ad_id,
                        "ad_name": ad_name,  # Prepend ad name
                        "ad_insights": extract_insights(ad.get("insights"))
                    }

        # Check for the 'next' field to determine if we need to paginate
        next_page_url = data.get("paging", {}).get("next")
        if next_page_url:
            # Update the base URL to the next page's URL and continue
            base_url = next_page_url
        else:
            break  # No more pages, we are done

    return {"data": all_campaign_data}


def extract_insights(insights):
    """
    Extract insights data or return an empty dict if no insights or data.
    """
    if insights and "data" in insights and len(insights["data"]) > 0:
        return insights["data"][0]  # Return the first item from the "data" array
    return {}  # Return an empty dictionary if no insights or data are available

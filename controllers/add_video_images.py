import base64
from io import BytesIO
import mimetypes
import requests
from PIL import Image
import re

def get_downloadable_drive_url(file_url):
    """
    Convert any Google Drive file URL to a downloadable link.
    Handles both shareable URLs and regular file URLs.
    Logs the extracted file ID and the final downloadable URL for debugging.
    """
    # Regex pattern to match various Google Drive file URL formats
    drive_url_pattern = r"https://drive\.google\.com/.*?file/d/([a-zA-Z0-9_-]+)"
    
    match = re.match(drive_url_pattern, file_url)

    if match:
        # Extract the file ID
        file_id = match.group(1)
        
        # Create the downloadable URL
        downloadable_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        # Log the extracted file ID and the final downloadable URL
        print(f"Extracted file ID: {file_id}")
        print(f"Generated downloadable URL: {downloadable_url}")
        
        # Return the downloadable link format for Google Drive
        return downloadable_url

    # If it's not a Google Drive link, return the original URL
    print(f"Input URL is not a Google Drive URL: {file_url}")
    return file_url


def add_video(ad_account_id, access_token, title, file_url):
    """
    Function to add a video to Facebook using the provided URL.
    
    :param ad_account_id: The Facebook Ad Account ID
    :param access_token: The Facebook API access token
    :param title: The title of the video
    :param file_url: The URL of the video (could be a Google Drive URL or another URL)
    :return: Response from the Facebook API
    """
    # Convert Google Drive URL to downloadable format if necessary
    downloadable_url = get_downloadable_drive_url(file_url)

    # Define the Facebook API URL for adding videos
    url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/advideos"
    
    # Define the headers for the request
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Define the data payload to send to the Facebook API
    video_data = {
        "title": title,
        "file_url": downloadable_url
    }
    
    # Make the POST request to Facebook API
    response = requests.post(url, headers=headers, json=video_data)
    
    # Return the response from the Facebook API
    return response.json()

def is_image_file(file_bytes):
    """
    Validate if the file is an image by checking its format using PIL.
    """
    try:
        image = Image.open(BytesIO(file_bytes))
        image.verify()  # Verify that the file is a valid image
        return True
    except Exception:
        return False

def add_ad_image(ad_account_id, access_token, file_url, image_name):
    """
    Upload an image from a URL to Facebook Marketing API in base64 format.
    """
    # Convert Google Drive URL to a downloadable format if necessary
    downloadable_url = get_downloadable_drive_url(file_url)
    # Download the image from the URL
    response = requests.get(downloadable_url)
    if response.status_code != 200:
        return {"error": "Failed to download image from the provided URL"}

    # Load the image and convert to base64
    image_bytes = BytesIO(response.content)
    try:
        image = Image.open(image_bytes)
        image_format = image.format.lower()  # e.g., 'jpeg', 'png'
        if image_format not in ["jpeg", "png"]:
            return {"error": f"Unsupported image format '{image_format}'. Only JPEG and PNG are allowed."}

        # Reset the image_bytes stream and convert to base64
        image_bytes.seek(0)
        image_base64 = base64.b64encode(image_bytes.read()).decode('utf-8')

    except Exception as e:
        return {"error": f"Invalid or corrupted image file. Error: {str(e)}"}

    # Define the Facebook API endpoint for uploading images
    upload_url = f"https://graph.facebook.com/v23.0/act_{ad_account_id}/adimages"

    # Payload with base64 image bytes
    payload = {
        "bytes": image_base64,
        "name": image_name
    }

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # Call the Facebook API
    response = requests.post(upload_url, headers=headers, json=payload)

    if response.status_code == 200:
        creative_image_url = response.json().get("images", {}).get(image_name, {}).get("url")
        image_url = {"image_url" : creative_image_url}
        return image_url
    else:
        return response.json()
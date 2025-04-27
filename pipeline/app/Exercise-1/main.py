import requests
import os
import zipfile
import shutil


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_file(url, download_folder=os.path.join(os.path.dirname(__file__), 'downloads')):
    f_name = url.split('/')[3]
    save_dir = os.path.join(download_folder, f_name) # Creating save directory

    try:
        resp = requests.get(url, stream=True)

        with open(save_dir, 'wb') as f:
            for content in resp.iter_content(chunk_size=8192):
                f.write(content)
    except:
        print('Loi trong qua trinh tai va luu file')


def unzip_delete(download_folder=os.path.join(os.path.dirname(__file__), 'downloads')):
    try:
        for file in os.listdir(download_folder):
            zip_dir = os.path.join(download_folder, file)
            try: 
                with zipfile.ZipFile(zip_dir, 'r') as zf:
                    zf.extractall(download_folder)
            except:
                print(f'Loi file {zip_dir}')

        for file in os.listdir(download_folder):
            if not file.endswith('.csv'):
                if os.path.isfile(os.path.join(download_folder, file)):
                    os.remove(os.path.join(download_folder, file))
                elif os.path.isdir(os.path.join(download_folder, file)):
                    shutil.rmtree(os.path.join(download_folder, file))
    except:
        print("Loi trong qua trinh giai nen")


def main(path=os.path.join(os.path.dirname(__file__), 'downloads')):
    # your code here
    # Creating 'downloads' file
    try:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
    except:
        print('Lỗi khi tạo file "downloads"')

    # Downloads files
    for url in download_uris:
        download_file(url)

    unzip_delete()
    



if __name__ == "__main__":
    main()
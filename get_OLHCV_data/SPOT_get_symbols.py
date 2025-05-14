import requests
import csv
import os


output_folder = "Symbols"

def get_symbols():
    # API endpoint URL
    url = "https://api.binance.com/api/v3/exchangeInfo"
    # url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    try:
        # Make the API request
        response = requests.get(url, headers=headers)
        print(f"Request URL: {url}")
        print(f"Status Code: {response.status_code}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            try:
                # Parse JSON response
                data = response.json()

                # Extract symbols from the response
                symbols = [symbol_info['symbol'] for symbol_info in data.get('symbols', [])]

                if not symbols:
                    print("No symbols found in the response.")
                    return

                # Filter symbols that end with 'USDT'
                filtered_symbols = [symbol for symbol in symbols if symbol.endswith('USDT')]

                if not filtered_symbols:
                    print("No symbols ending with 'USDT' found.")
                    return

                # Ensure the output directory exists
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)
                    print(f"Created output directory: {output_folder}")

                # Define the path for the filtered CSV file
                filtered_csv_file_path = os.path.join(output_folder, 'spot_binance_symbols.csv')

                # Write filtered symbols to the CSV file
                with open(filtered_csv_file_path, 'w', newline='') as filtered_csv_file:
                    writer = csv.writer(filtered_csv_file)
                    for symbol in filtered_symbols:
                        writer.writerow([symbol])

                print(f"Filtered symbols ending with 'USDT' written to {filtered_csv_file_path}")

            except ValueError as e:
                print(f"Error parsing JSON response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")

if __name__ == "__main__":
    get_symbols()

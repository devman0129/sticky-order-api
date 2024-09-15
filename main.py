import requests
from requests.auth import HTTPBasicAuth

url = 'https://willpower.sticky.io/api/v2/orders/histories?start_at=2024-08-01 23:59:59&end_at=2024-08-03 00:00:00'

username = 'borysklymenko'
password = 'f3UXF6ypnUzQ'

base_url = "https://willpower.sticky.io/api"

def handle_request(url, method, data, username, password, headers):
    if method == "GET":
        response = requests.get(url, auth=HTTPBasicAuth(username, password), headers=headers)
    else:
        response = requests.post(url, json=data, auth=HTTPBasicAuth(username, password), headers=headers)
    
    return response.json()


def main():

    start_date = "2023-01-01"
    end_date = "2023-01-31"

    ######## Get Page Number 
    order_history_url = f"{base_url}/v2/orders/histories?start_at={start_date} 23:59:59&end_at={end_date} 00:00:00"
    order_history_res = handle_request(order_history_url, "GET", None, username, password, None)
    page_number = order_history_res["last_page"]
    print(f"\n----------------------------------------------------- total page number : {page_number} ------------------------------------------------------\n")
    order_data = []

    for page_index in range(1, page_number+1):
        print(f"\n--------------- handling page index : {page_index} ------------------\n")
        order_history_url_page = f"{order_history_url}&page={page_index}"
        order_per_page = handle_request(order_history_url_page, "GET", None, username, password, None)
        order_data = order_data + order_per_page["data"]
    
    processed_order_data = []

    for order in order_data:
        processed_order_data.append({
            "order_id": order["order_id"],
            "created_at": order["created_at"]["date"]
        })
    
    ## Remove duplicates based on order_id
    unique_processed_order_data = list({order['order_id']: order for order in processed_order_data}.values())

    print(f"\nlen of unique processed order data : {len(unique_processed_order_data)}\n")

    ## Get Order Details
    order_view_url = "https://willpower.sticky.io/api/v1/order_view"
    total_order_ids = [order['order_id'] for order in unique_processed_order_data]

    bulk_order_view_num = 500

    total_id_list_len = len(total_order_ids)
    
    for view_index in range(0, total_id_list_len, bulk_order_view_num):
        start_view_index = view_index
        end_view_index = view_index + bulk_order_view_num
        if end_view_index > total_id_list_len:
            end_view_index = total_id_list_len
        
        order_ids = total_order_ids[start_view_index:end_view_index]

        data = {
            "order_id": order_ids
        }

        order_details = handle_request(order_view_url, "POST", data, username, password, None)

        order_ids_str = order_details["order_id"]
        order_details_data = order_details["data"]

        print(f"\nlen of order details data : {len(order_details_data)}\n")


        final_result = []
        for order_id_str in order_ids_str:
            if order_id_str in order_details_data:
                every_order_detail = order_details_data[order_id_str]
                created_at_value = next((order['created_at'] for order in unique_processed_order_data if order['order_id'] == int(order_id_str)), None)
                final_result.append({
                    "order_id": order_id_str,
                    "order_total": every_order_detail["order_total"],
                    "campaign_id": every_order_detail["campaign_id"],
                    "utm_medium": every_order_detail["utm_info"]["medium"],
                    "utm_campaign": every_order_detail["utm_info"]["campaign"],
                    "utm_source": every_order_detail["utm_info"]["source"],
                    "utm_term": every_order_detail["utm_info"]["term"],
                    "utm_content": every_order_detail["utm_info"]["content"],
                    "created_at": created_at_value
                })
        
        retool_url = 'https://api.retool.com/v1/workflows/9f208667-2747-43dd-8e45-75e2882ec84d/startTrigger'
        headers = {
            'X-Workflow-Api-Key': 'retool_wk_d683f5a2c12141b1b8b7f0bb45c820d2'
        }

        bulk_number = 500
        total_order_number = len(final_result)
        
        for index in range(0, total_order_number, bulk_number):
            start_index = index
            end_index = index + bulk_number
            if end_index > total_order_number:
                end_index=total_order_number
            
            bulk_orders = final_result[start_index:end_index]

            data = {
                "orders": bulk_orders
            }
            print(f"\n view_index : ({start_view_index}, {end_view_index}), ------- start_index : {start_index}, end_index: {end_index}---------\n")
            response = requests.post(retool_url, json=data, headers=headers)
            print(response.json())




if __name__ == "__main__":
    main()



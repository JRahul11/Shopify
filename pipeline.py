
import pytz
from datetime import datetime
from dotenv import dotenv_values
import firebase_admin
from firebase_admin import credentials, firestore


# Load ENV variables
env_vars = dotenv_values(".env")
master_collection = env_vars['MASTER_COLLECTION']
shopify_collection = env_vars['SHOPIFY_COLLECTION']
shiprocket_collection = env_vars['SHIPROCKET_COLLECTION']
nimbus_collection = env_vars['NIMBUS_COLLECTION']


# Initialize Firestore collection instance 
credential = credentials.Certificate('e-commerce-analytics-390718-b9291d157d49.json')
firebase_admin.initialize_app(credential)
db = firestore.client()
master_collection_ref = db.collection(master_collection)
shopify_collection_ref = db.collection(shopify_collection)
shiprocket_collection_ref = db.collection(shiprocket_collection)
nimbus_collection_ref = db.collection(nimbus_collection)

current_timestamp = datetime.now(pytz.UTC)


# Get Last Load TimeStamp from Master Collection
last_load_timestamp = master_collection_ref.document('meta_data').get().to_dict()['last_load_timestamp']
print(last_load_timestamp)


# # Get new order_ids from each collection
# def getNewOrderIds(collection_ref):
#     doc_ids = []
#     docs = collection_ref.get()
#     for doc in docs:
#         # Get only new ids
#         # last_updated_at = doc.to_dict()['updated_at']
#         # if last_updated_at > last_load_timestamp:
#         doc_ids.append(doc.id)
#     return doc_ids

    
    
# shopify_doc_ids = getNewOrderIds(shopify_collection_ref)
# shiprocket_doc_ids = getNewOrderIds(shiprocket_collection_ref)
# nimbus_doc_ids = getNewOrderIds(nimbus_collection_ref)

# common order ids
# common_order_ids = shopify_doc_ids.intersection(shiprocket_doc_ids, nimbus_doc_ids)

for doc in shopify_collection_ref.get():
    #! Add check for LLT 
    if not master_collection_ref.document(doc.id).get().exists:
        data = {
            'channel_name': None,
            'order_date': None,
            'order_id': doc.id,
            'shipment_date': None,
            'awb_number': None,
            'courier_name': None,
            'shipping_status': None,
            'ndr_status': None,
            'last_status_updated_date': None,
            'payment_type': None,
            'order_value': None,
            'tax': None,
            'discount': None,
            'order_tags': None,
            'input_weight': None,
            'charged_weight': None,
            'dead_weight': None,
            'volumetric_weight': None,
            'freight_charge': None,
            'cod_charge': None,
            'rto_charge': None,
            'freight_charge_overcharge': None,
            'rto_charge_overcharge': None,
            'total_shipping_charges': None,
            'zone': None,
            'pickup_location_selected': None,
            'cod_remittance_status': None,
            'pickup_date': None,
            'out_of_delivary_date': None,
            'rto_delivery_date': None,
            'rto_awb': None,
            'tat_compliance': None,
            'pod_status': None,
            'pod_date': None,
            'length': None,
            'breadth': None,
            'height': None,
            'customer_name': None,
            'customer_email': None,
            'customer_phone_number': None,
            'address': None,
            'area_name': None,
            'city': None,
            'state': None,
            'pincode': None,
            'country': None,
            'product_1': None,
            'qty_1': None,
            'sku_1': None,
            'purchase_cost_1': None,
            'product_2': None,
            'qty_2': None,
            'sku_2': None,
            'purchase_cost_2': None,
            'channel_url': None,
            'gateway_name': None,
            'referring_site': None
        }
        # master_collection_ref.document(doc.id).set(data)
    else:
        # master_collection_ref.document(doc.id).delete()
        pass



for shiprocket_doc in shiprocket_collection_ref.get():
    shiprocket_doc_values = shiprocket_doc.to_dict()
    last_updated_at = shiprocket_doc_values['createdTime']
    if last_updated_at > last_load_timestamp:
        master_doc_ref = master_collection_ref.document(shiprocket_doc.id)
        if master_doc_ref.get().exists:
            fields = {
                'channel_name': shiprocket_doc_values['channel_name'],
                # 'order_date': None,
                # 'shipment_date' : None,
                'awb_number': shiprocket_doc_values['shipments'][0]['awb'],
                'courier_name': shiprocket_doc_values['shipments'][0]['courier'],
                'shipping_status': shiprocket_doc_values['status'],
                # 'ndr_status': None,
                'last_status_updated_date': shiprocket_doc_values['updated_at'],
                'payment_type': shiprocket_doc_values['payment_method'],
                'order_value': shiprocket_doc_values['total_order_value'],
                'tax': shiprocket_doc_values['tax'],
                'discount': shiprocket_doc_values['others']['discount_codes'],
                'order_tags': shiprocket_doc_values['order_tag'],
                # 'input_weight': None,
                'charged_weight': shiprocket_doc_values['awb_data']['charges']['charged_weight'],
                'dead_weight': shiprocket_doc_values['total_dead_weight'],
                'volumetric_weight': shiprocket_doc_values['total_volumetric_weight'],
                'freight_charge': shiprocket_doc_values['awb_data']['charges']['freight_charges'],
                'cod_charge': shiprocket_doc_values['awb_data']['charges']['cod_charges'],
                # 'rto_charge': None,
                # 'freight_charge_overcharge': None,
                # 'rto_charge_overcharge': None,
                'total_shipping_charges': shiprocket_doc_values['total'],
                'zone': shiprocket_doc_values['zone'],
                'pickup_location_selected': shiprocket_doc_values['pickup_location'],
                # 'cod_remittance_status': None,
                'pickup_date': shiprocket_doc_values['shipments'][0]['pickup_scheduled_date'],
                'out_of_delivary_date': shiprocket_doc_values['out_for_delivery_date'],
                'rto_delivery_date': shiprocket_doc_values['shipments'][0]['rto_delivered_date'],
                'rto_awb': shiprocket_doc_values['shipments'][0]['rto_awb'],
                # 'tat_compliance': None,
                'pod_status': shiprocket_doc_values['shipments'][0]['pod'],
                # 'pod_date': None,
                # 'length': None,
                # 'breadth': None,
                # 'height': None,
                'customer_name': shiprocket_doc_values['customer_name'],
                'customer_email': shiprocket_doc_values['customer_email'],
                'customer_phone_number': shiprocket_doc_values['customer_phone'],
                'address': shiprocket_doc_values['customer_address'],
                # 'area_name': None,
                'city': shiprocket_doc_values['customer_city'],
                'state': shiprocket_doc_values['customer_state'],
                'pincode': shiprocket_doc_values['customer_pincode'],
                'country': shiprocket_doc_values['customer_country'],
                'product_1': shiprocket_doc_values['products'][0]['name'],
                'qty_1': shiprocket_doc_values['products'][0]['quantity'],
                'sku_1': shiprocket_doc_values['products'][0]['channel_sku'],
                'purchase_cost_1': shiprocket_doc_values['products'][0]['product_cost'],
                # 'product_2': shiprocket_doc_values['products'][1]['name'],
                # 'qty_2': shiprocket_doc_values['products'][1]['quantity'],
                # 'sku_2': shiprocket_doc_values['products'][1]['channel_sku'],
                # 'purchase_cost_2': shiprocket_doc_values['products'][1]['product_cost'],
                'channel_url': shiprocket_doc_values['others']['order_status_url'],
                'gateway_name': shiprocket_doc_values['others']['gateway'],
                'referring_site': shiprocket_doc_values['others']['referring_site']
            }
            master_doc_ref.update(fields)
            # master_collection_ref.document(shiprocket_doc.id).delete()
        else:
            data = {
                'channel_name': shiprocket_doc_values['channel_name'],
                'order_date': None,
                'order_id': shiprocket_doc.id,
                'shipment_date' : None,
                'awb_number': shiprocket_doc_values['shipments'][0]['awb'],
                'courier_name': shiprocket_doc_values['shipments'][0]['courier'],
                'shipping_status': shiprocket_doc_values['status'],
                'ndr_status': None,
                'last_status_updated_date': shiprocket_doc_values['updated_at'],
                'payment_type': shiprocket_doc_values['payment_method'],
                'order_value': shiprocket_doc_values['total_order_value'],
                'tax': shiprocket_doc_values['tax'],
                'discount': shiprocket_doc_values['others']['discount_codes'],
                'order_tags': shiprocket_doc_values['order_tag'],
                'input_weight': None,
                'charged_weight': shiprocket_doc_values['awb_data']['charges']['charged_weight'],
                'dead_weight': shiprocket_doc_values['total_dead_weight'],
                'volumetric_weight': shiprocket_doc_values['total_volumetric_weight'],
                'freight_charge': shiprocket_doc_values['awb_data']['charges']['freight_charges'],
                'cod_charge': shiprocket_doc_values['awb_data']['charges']['cod_charges'],
                'rto_charge': None,
                'freight_charge_overcharge': None,
                'rto_charge_overcharge': None,
                'total_shipping_charges': shiprocket_doc_values['total'],
                'zone': shiprocket_doc_values['zone'],
                'pickup_location_selected': shiprocket_doc_values['pickup_location'],
                'cod_remittance_status': None,
                'pickup_date': shiprocket_doc_values['shipments'][0]['pickup_scheduled_date'],
                'out_of_delivary_date': shiprocket_doc_values['out_for_delivery_date'],
                'rto_delivery_date': shiprocket_doc_values['shipments'][0]['rto_delivered_date'],
                'rto_awb': shiprocket_doc_values['shipments'][0]['rto_awb'],
                'tat_compliance': None,
                'pod_status': shiprocket_doc_values['shipments'][0]['pod'],
                'pod_date': None,
                'length': None,
                'breadth': None,
                'height': None,
                'customer_name': shiprocket_doc_values['customer_name'],
                'customer_email': shiprocket_doc_values['customer_email'],
                'customer_phone_number': shiprocket_doc_values['customer_phone'],
                'address': shiprocket_doc_values['customer_address'],
                'area_name': None,
                'city': shiprocket_doc_values['customer_city'],
                'state': shiprocket_doc_values['customer_state'],
                'pincode': shiprocket_doc_values['customer_pincode'],
                'country': shiprocket_doc_values['customer_country'],
                'product_1': shiprocket_doc_values['products'][0]['name'],
                'qty_1': shiprocket_doc_values['products'][0]['quantity'],
                'sku_1': shiprocket_doc_values['products'][0]['channel_sku'],
                'purchase_cost_1': shiprocket_doc_values['products'][0]['product_cost'],
                'product_2': shiprocket_doc_values['products'][1]['name'],
                'qty_2': shiprocket_doc_values['products'][1]['quantity'],
                'sku_2': shiprocket_doc_values['products'][1]['channel_sku'],
                'purchase_cost_2': shiprocket_doc_values['products'][1]['product_cost'],
                'channel_url': shiprocket_doc_values['others']['order_status_url'],
                'gateway_name': shiprocket_doc_values['others']['gateway'],
                'referring_site': shiprocket_doc_values['others']['referring_site']
            }
            master_collection_ref.document(shiprocket_doc.id).set(data)
    else:
        print("Already added to master")


# #! UNCOMMENT FROM HERE
# for shiprocket_doc in shiprocket_collection_ref.get():
#     #! Add check for LLT 
#     shiprocket_doc_values = shiprocket_doc.to_dict()
#     master_doc_ref = master_collection_ref.document(shiprocket_doc.id)
#     if master_doc_ref.get().exists:
#         fields = {
#             'channel_name': shiprocket_doc_values['channel_name'],
#             'awb_number': shiprocket_doc_values['shipments'][0]['awb'],
#             'courier_name': shiprocket_doc_values['shipments'][0]['courier'],
#             'shipping_status': shiprocket_doc_values['status'],
#             'last_status_updated_date': shiprocket_doc_values['update_at'],
#             'payment_type': shiprocket_doc_values['payment_method'],
#             'order_value': shiprocket_doc_values['total_order_value'],
#             'tax': shiprocket_doc_values['tax'],
#             'discount': shiprocket_doc_values['others']['discount_codes'],
#             'order_tags': shiprocket_doc_values['order_tag'],
#             'charged_weight': shiprocket_doc_values['awb_data']['charges']['charged_weight'],
#             'dead_weight': shiprocket_doc_values['total_dead_weight'],
#             'volumetric_weight': shiprocket_doc_values['total_volumetric_weight'],
#             'freight_charge': shiprocket_doc_values['awb_data']['charges']['freight_charges'],
#             'cod_charge': shiprocket_doc_values['awb_data']['charges']['cod_charges'],
#             'total_shipping_charges': shiprocket_doc_values['total'],
#             'zone': shiprocket_doc_values['zone'],
#             'pickup_location_selected': shiprocket_doc_values['pickup_location'],
#             'pickup_date': shiprocket_doc_values['shipments']['pickup_scheduled_date'],
#             'out_of_delivary_date': shiprocket_doc_values['out_for_delivery_date'],
#             'rto_delivery_date': shiprocket_doc_values['shipments']['rto_delivered_date'],
#             'rto_awb': shiprocket_doc_values['shipments']['rto_awb'],
#             'pod_status': shiprocket_doc_values['pod'],
#             'customer_name': shiprocket_doc_values['customer_name'],
#             'customer_email': shiprocket_doc_values['customer_email'],
#             'customer_phone_number': shiprocket_doc_values['customer_phone'],
#             'address': shiprocket_doc_values['customer_address'],
#             'city': shiprocket_doc_values['customer_city'],
#             'state': shiprocket_doc_values['customer_state'],
#             'pincode': shiprocket_doc_values['customer_pincode'],
#             'country': shiprocket_doc_values['customer_country'],
#             'product_1': shiprocket_doc_values['products'][0]['name'],
#             'qty_1': shiprocket_doc_values['products'][0]['quantity'],
#             'sku_1': shiprocket_doc_values['products'][0]['channel_sku'],
#             'purchase_cost_1': shiprocket_doc_values['products'][0]['product_cost'],
#             'product_2': shiprocket_doc_values['products'][1]['name'],
#             'qty_2': shiprocket_doc_values['products'][1]['quantity'],
#             'sku_2': shiprocket_doc_values['products'][1]['channel_sku'],
#             'purchase_cost_2': shiprocket_doc_values['products'][1]['product_cost'],
#             'channel_url': shiprocket_doc_values['others']['order_status_url'],
#             # 'shipment_date' : None,
#             # 'ndr_status': None,
#             # 'input_weight': None,
#             # 'rto_charge': None,
#             # 'freight_charge_overcharge': None,
#             # 'rto_charge_overcharge': None,
#             # 'cod_remittance_status': None,
#             # 'tat_compliance': None,
#             # 'pod_date': None,
#             # 'length': None,
#             # 'breadth': None,
#             # 'height': None,
#             # 'area_name': None,
#             # 'gateway_name': None,
#             # 'referring_site': None
#         }
#         master_doc_ref.update(fields)
#     else:
#         # Create new document like shopify
#         print('New document for shiprocket')


# for nimbus_doc in nimbus_collection_ref.get():
#     nimbus_doc_values = nimbus_doc.to_dict()
#     master_doc_ref = master_collection_ref.document(str(nimbus_doc_values['id']))
#     if master_doc_ref.get().exists:
#         fields = {
#             'shipment_date' : nimbus_doc_values['shipped_date'],
#             'awb_number': nimbus_doc_values['awb_number'],
#             'shipping_status': nimbus_doc_values['status'],
#             'pickup_date': nimbus_doc_values['pickup_date'],
#             'rto_awb': nimbus_doc_values['rto_awb'],
#             'channel_name': None,
#             'courier_name': None,
#             'ndr_status': None,
#             'last_status_updated_date': None,
#             'payment_type': None,
#             'order_value': None,
#             'tax': None,
#             'discount': None,
#             'order_tags': None,
#             'input_weight': None,
#             'charged_weight': None,
#             'dead_weight': None,
#             'volumetric_weight': None,
#             'freight_charge': None,
#             'cod_charge':None,
#             'rto_charge': None,
#             'freight_charge_overcharge': None,
#             'rto_charge_overcharge': None,
#             'total_shipping_charges': None,
#             'zone': None,
#             'pickup_location_selected': None,
#             'cod_remittance_status': None,
#             'out_of_delivary_date': None,
#             'rto_delivery_date': None,
#             'tat_compliance': None,
#             'pod_status': None,
#             'pod_date': None,
#             'length': None,
#             'breadth': None,
#             'height': None,
#             'customer_name': None,
#             'customer_email': None,
#             'customer_phone_number': None,
#             'address': None,
#             'area_name': None,
#             'city': None,
#             'state': None,
#             'pincode': None,
#             'country': None,
#             'product_1': None,
#             'qty_1': None,
#             'sku_1': None,
#             'purchase_cost_1': None,
#             'product_2': None,
#             'qty_2': None,
#             'sku_2': None,
#             'purchase_cost_2': None,
#             'channel_url': None,
#             'gateway_name': None,
#             'referring_site': None
#         }
#         master_doc_ref.update(fields)
#     else:
#         print('New document for nimbus')







# if datetime.now(pytz.UTC) > temp:
#     collection_ref.document('meta_data').update({'last_load_timestamp':datetime.now(pytz.UTC)})
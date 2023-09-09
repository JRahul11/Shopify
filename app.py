from flask import Flask, jsonify
import firebase_admin
from firebase_admin import credentials, firestore


cred = credentials.Certificate('e-commerce-analytics-390718-b9291d157d49.json')
firebase_admin.initialize_app(cred)
db = firestore.client()
app = Flask(__name__)



@app.route('/rto_productwise_analysis', methods=['GET'])
def rto_productwise_analysis():
    
    collection_ref = db.collection('master')
    docs = collection_ref.get()
    product_counts = {}
    
    for doc in docs:
        doc = doc.to_dict()
        product = doc.get('product_1')
        status = doc.get('shipping_status')
        
        if product not in product_counts:
            product_counts[product] = {'Delivered': 0, 'RTO': 0}

        if status == 'Delivered':
            product_counts[product]['Delivered'] += 1
        elif status == 'RTO':
            product_counts[product]['RTO'] += 1
    
    for product, statuses in product_counts.items():
        total = statuses['Delivered'] + statuses['RTO']
        statuses['Delivered_percentage'] = (statuses['Delivered'] / total) * 100
        statuses['RTO_percentage'] = (statuses['RTO'] / total) * 100

    return jsonify(product_counts)


if __name__ == '__main__':
    app.run(debug=True)



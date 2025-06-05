import os
import src
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/health')
def health():
    """Endpoint для healthcheck"""
    return jsonify({"status": "ok", "module": os.getenv('MODULE_NAME')}), 200

if __name__ == '__main__':
    # Инициализация основного приложения
    src.main()
    
    # Для сервисов, которые используют Flask
    if os.getenv('MODULE_NAME') in ['customer', 'base', 'planningsystem']:
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8000)))
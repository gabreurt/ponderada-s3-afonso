from flask import Flask, request, jsonify
import pandas as pd

from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import prepare_dataframe_for_insert

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("transaction-id")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')

@app.route('/upload_csv', methods=['POST'])
def receive_csv():
    if 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "Nenhum arquivo selecionado"}), 400

    try:
        # Ler o arquivo CSV em um DataFrame
        df = pd.read_csv(file)

        # Salvar o DataFrame como um arquivo Parquet temporário
        filename = f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(filename)

        # Upload para o MinIO
        upload_file("transaction-id", filename)

        # Ler o arquivo Parquet do MinIO
        download_file("transaction-id", filename, f"downloaded_{filename}")
        df_parquet = pd.read_parquet(f"downloaded_{filename}")

        # Preparar e inserir dados no ClickHouse
        df_prepared = prepare_dataframe_for_insert(df_parquet)
        client = get_client()  # Obter o cliente ClickHouse
        insert_dataframe(client, 'working_data', df_prepared)

        return jsonify({"message": "Arquivo CSV recebido, processado e armazenado com sucesso"}), 200

    except Exception as e:
        # Logar o erro para fins de depuração
        print(f"Erro durante o processamento: {e}")
        return jsonify({"error": "Erro no processamento do arquivo CSV"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

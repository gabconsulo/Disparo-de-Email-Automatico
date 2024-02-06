import mysql.connector
from mysql.connector import errorcode
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import schedule
import time
import sys

def conectar_banco():
    config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'database',
        'raise_on_warnings': True
    }

    try:
        conn = mysql.connector.connect(**config)

        if conn.is_connected():
            print(f"Conectado ao banco de dados 'database'")
            
        return conn

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Erro: Usuário ou senha incorretos")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Erro: Banco de dados não existe")
        else:
            print(f"Erro: {err}")
        return None

def enviar_email(destinatario, assunto, corpo):
    servidor_email = 'servidordoemail.com.br'
    porta_email = 587
    usuario_email = 'seuemail@gmail.com'
    senha_email = 'suasenha'

    mensagem = MIMEMultipart()
    mensagem.attach(MIMEText(corpo, 'plain'))

    mensagem['From'] = usuario_email
    mensagem['To'] = destinatario
    mensagem['Subject'] = assunto

    servidor_smtp = smtplib.SMTP(host=servidor_email, port=porta_email)
    servidor_smtp.starttls()
    servidor_smtp.login(usuario_email, senha_email)

    servidor_smtp.sendmail(usuario_email, destinatario, mensagem.as_string())

    servidor_smtp.quit()

def job():
    print("Iniciando envio de e-mails...")

    conn = conectar_banco()

    if conn:
        try:
            cursor = conn.cursor()

            query = """
                SELECT
                    estudantes.EST_STR_CPF,
                    estudantes.EST_STR_NOME,
                    estagios.ESTA_STR_EMAIL_SUPERVISOR,
                    estagios.ESTA_DTM_RENOVA1,
                    estagios.ESTA_DTM_RENOVA2,
                    estagios.ESTA_DTM_RENOVA3,
                    CASE
                        WHEN estagios.ESTA_DTM_RENOVA1 IS NOT NULL AND DATE(estagios.ESTA_DTM_RENOVA1) = DATE_ADD(CURDATE(), INTERVAL 30 DAY) THEN '1'
                        WHEN estagios.ESTA_DTM_RENOVA2 IS NOT NULL AND DATE(estagios.ESTA_DTM_RENOVA2) = DATE_ADD(CURDATE(), INTERVAL 30 DAY) THEN '2'
                        WHEN estagios.ESTA_DTM_RENOVA3 IS NOT NULL AND DATE(estagios.ESTA_DTM_RENOVA3) = DATE_ADD(CURDATE(), INTERVAL 30 DAY) THEN '3'
                        WHEN estagios.ESTA_DTM_DTFIMCONTRATO IS NOT NULL AND DATE(estagios.ESTA_DTM_DTFIMCONTRATO)  = DATE_ADD(CURDATE(), INTERVAL 30 DAY) THEN '4'
                        ELSE 'contrato com prazo maior do que 30 dias'
                    END AS status
                FROM estagios
                INNER JOIN estudantes ON estudantes.EST_INT_CODIGO = estagios.EST_INT_CODIGO
                WHERE
                    (
                        (estagios.ESTA_DTM_RENOVA1 IS NOT NULL AND DATE(estagios.ESTA_DTM_RENOVA1) = DATE_ADD(CURDATE(), INTERVAL 30 DAY))
                        AND
                        (estagios.ESTA_DTM_DTFIM IS NULL OR estagios.ESTA_DTM_DTFIM >= CURDATE() ) AND estagios.ESTA_DTM_DTINICIO <= CURDATE()
                    )
                    OR
                    (
                        (estagios.ESTA_DTM_RENOVA2 IS NOT NULL AND DATE(estagios.ESTA_DTM_RENOVA2) = DATE_ADD(CURDATE(), INTERVAL 30 DAY))
                        AND
                        (estagios.ESTA_DTM_DTFIM IS NULL OR estagios.ESTA_DTM_DTFIM >= CURDATE() ) AND estagios.ESTA_DTM_DTINICIO <= CURDATE()
                    )
                    OR
                    (
                        (estagios.ESTA_DTM_RENOVA3 IS NOT NULL AND DATE(estagios.ESTA_DTM_RENOVA3) = DATE_ADD(CURDATE(), INTERVAL 30 DAY))
                        AND
                        (estagios.ESTA_DTM_DTFIM IS NULL OR estagios.ESTA_DTM_DTFIM >= CURDATE() ) AND estagios.ESTA_DTM_DTINICIO <= CURDATE()
                    )
                    OR
                    (
                        (estagios.ESTA_DTM_DTFIMCONTRATO IS NOT NULL AND DATE(estagios.ESTA_DTM_DTFIMCONTRATO)  = DATE_ADD(CURDATE(), INTERVAL 30 DAY))
                        AND
                        (estagios.ESTA_DTM_DTFIM IS NULL OR estagios.ESTA_DTM_DTFIM >= CURDATE() ) AND estagios.ESTA_DTM_DTINICIO <= CURDATE()
                    )
                ORDER BY status ASC;

            """

            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                cpf = row[0]
                nome = row[1]
                destinatario_supervisor = row[2]
                status = row[5]

                corpo_email = f"Prezado supervisor,\n\nO estudante {nome} (CPF: {cpf}) possui um contrato com status {status}.\n\nAtenciosamente,\nSua Aplicação"

                assunto_email = 'Status do Contrato do Estudante'

                enviar_email(destinatario_supervisor, assunto_email, corpo_email)

            print("Envio de e-mails concluído.")

        except mysql.connector.Error as err:
            print(f"Erro ao executar a consulta: {err}")

        finally:
            if 'cursor' in locals():
                cursor.close()

            if 'conn' in locals() and conn.is_connected():
                conn.close()
                print("Conexão fechada.")
                global conexao_fechada
                conexao_fechada = True

conexao_fechada = False

# Agende a execução diária às 8:00 AM
schedule.every().day.at("15:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

    if conexao_fechada:
        break
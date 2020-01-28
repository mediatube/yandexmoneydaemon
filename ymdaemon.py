import asyncio
from secret import ENV_CLIENT_ID
from aiohttp import web, ClientSession
from aiohttp.formdata import FormData
import sqlite3
import billing_service
from redis import Redis
from rq import Queue
from rq_access import *
import logging

redis_conn = Redis(host=rq_host, port=rq_port, password=rq_password)
post_data_last = None

q_billing = Queue(connection=redis_conn, name='billing', default_timeout=10)
q_billingv = Queue(connection=redis_conn, name='billingv', default_timeout=10)


# cursor.execute("""CREATE TABLE session
#                   (access_token text, client_secret text, redirect_uri text, client_id text)
#                """)
# conn.commit()


class YMClientSession(object):
    def __init__(self, client_id: str, db_filename: str = "ymdaemon.sqlite"):
        self.client_id = client_id
        self.db_filename = db_filename
        self.conn = sqlite3.connect(self.db_filename)  # или :memory: чтобы сохранить в RAM
        self.cursor = self.conn.cursor()
        self.client_secret = None
        self.redirect_uri = None
        self.access_token = None
        self.scope = ['account-info', 'operation-history', 'operation-details']
        self.init_from_db()
        self.oauth_base_url = 'https://money.yandex.ru/oauth/'
        self.oauth_url = {'authorize': f'{self.oauth_base_url}authorize',
                          'token': f'{self.oauth_base_url}token'}
        self.authorize_url = f"{self.oauth_url['authorize']}?client_id={self.client_id}" \
                             f"&redirect_uri={self.redirect_uri}" \
                             f"&scope={'+'.join(x for x in self.scope)}" \
                             f"&response_type=code"

    def init_from_db(self):
        self.cursor.execute("SELECT client_secret, redirect_uri, access_token FROM session WHERE client_id=?;",
                            [self.client_id])
        self.client_secret, self.redirect_uri, self.access_token = self.cursor.fetchone()

    def update_token(self, access_token: str):
        self.cursor.execute("UPDATE session SET access_token=? WHERE client_id=?;", [access_token, self.client_id])
        self.conn.commit()
        self.init_from_db()

    def check_insert_operation(self, operation: dict):
        if 'label' not in operation.keys():
            return
        # print(operation)
        self.cursor.execute(
            "INSERT OR IGNORE INTO operations (id,label,amount,datetime,is_processed) VALUES (?,?,?,?,?);",
            [int(operation['operation_id']),
             operation['label'],
             operation['amount'],
             operation['datetime'],
             0])
        self.conn.commit()

    def get_operation_from_db(self, id: int):
        self.cursor.execute("SELECT label, id, datetime FROM operations WHERE id=?;", [id])
        return self.cursor.fetchone()

    def pop_unprocessed_operation_id(self):
        self.cursor.execute("SELECT id FROM operations WHERE is_processed=0;")
        result = self.cursor.fetchone()
        id = result[0] if result else None
        return id

    async def process_operation(self, id: int):
        print(self.get_operation_from_db(id))
        label, operation_id, datetime = self.get_operation_from_db(id)
        job_dl = q_billing.enqueue(billing_service.successful_payment_callback, label, f"{operation_id}a", datetime)
        while job_dl.result is None:
            job_dl.refresh()
            if job_dl.is_failed:
                raise Exception
        job_dlv = q_billingv.enqueue(billing_service.successful_payment_callback, label, f"{operation_id}v", datetime)
        while job_dlv.result is None:
            job_dlv.refresh()
            if job_dlv.is_failed:
                raise Exception
        self.cursor.execute("UPDATE operations SET is_processed=1 WHERE id=?;", [id])
        self.conn.commit()


client = YMClientSession(ENV_CLIENT_ID)
print(client.authorize_url)
# cursor.execute("SELECT redirect_uri FROM session WHERE client_id=?", [ENV_CLIENT_ID])
# ENV_REDIRECT_URI = cursor.fetchone()
# exit(0)
# cursor.execute("""CREATE TABLE session
#                   (access_token text, client_secret text, redirect_uri text, client_id text)
#                """)
# client.cursor.execute("""CREATE TABLE operations
#                   (operation_id integer , invoice_label text, amount text, datetime text, is_processed integer)
#                """)
# client.conn.commit()
API_URL_OPERATION_HISTORY = 'https://money.yandex.ru/api/operation-history'
MAX_QUEUE_SIZE = 100
HOST_PORT = 19998
HOST_ADDR = '127.0.0.1'
POLLING_DELAY = 30.0

routes = web.RouteTableDef()
app = web.Application()
requests_pool = []
lock = asyncio.Lock()
event = asyncio.Event()


async def on_startup(app):
    loop = asyncio.get_event_loop()
    loop.create_task(poll_operation_history(delay=POLLING_DELAY))


async def poll_operation_history(delay: float = 0.1, records: int = 5):
    json_response = {}
    async with ClientSession() as session:
        while True:
            post_headers = {'Host': 'money.yandex.ru',
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Authorization': f'Bearer {client.access_token}'}
            post_payload = FormData([('records', records), ('type', 'deposition'), ('details', 'false')])
            async with session.post(f'{API_URL_OPERATION_HISTORY}', data=post_payload, headers=post_headers) as resp:
                # print(resp.status)
                # print(await resp.text())
                if resp.status == 200:
                    json_response = await resp.json()
                    for operation in json_response['operations']:
                        client.check_insert_operation(operation)
            while True:
                unprocessed_operation_id = client.pop_unprocessed_operation_id()
                if unprocessed_operation_id:
                    try:
                        await client.process_operation(unprocessed_operation_id)
                    except Exception as e:
                        print(e)
                    await asyncio.sleep(1)
                else:
                    break
            await asyncio.sleep(delay)


@routes.post('/')
async def handle_post(request):
    return web.Response(status=200)


@routes.get('/')
async def handle_get(request):
    print(request)
    if 'code' in request.rel_url.query:
        code = request.rel_url.query['code']
        async with ClientSession() as session:
            async with session.post(f"{client.oauth_url['token']}?code={code}"
                                    f"&client_id={client.client_id}"
                                    f"&grant_type=authorization_code"
                                    f"&redirect_uri={client.redirect_uri}"
                                    f"&client_secret={client.client_secret}") as resp:
                print(resp.status)
                print(await resp.text())
                if resp.status == 200:
                    resp_json = await resp.json()
                    client.update_token(resp_json['access_token'])
        return web.Response(status=200)


if __name__ == '__main__':
    app.router.add_routes(routes)
    app.on_startup.append(on_startup)
    web.run_app(app, host=HOST_ADDR, port=HOST_PORT)

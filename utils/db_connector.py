import oracledb
import yaml

def get_connection():
    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)["oracle"]
    
    conn = oracledb.connect(
        user=cfg["user"],
        password=cfg["password"],
        dsn=f"{cfg['host']}:{cfg['port']}/{cfg['service_name']}"
    )
    # IMPORTANT: activer autocommit pour que les INSERT/DELETE/UPDATE soient persistés immédiatement
    conn.autocommit = True
    return conn

def execute_query(sql):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]
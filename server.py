#!/usr/bin/env python3
"""
FillGap CRM - Ventas y Oportunidades
Versión 2.0 - Con usuarios, empresas, personas, tareas
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json, os, hashlib, secrets
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    import psycopg2, psycopg2.extras
    def get_conn():
        return psycopg2.connect(DATABASE_URL)
    IS_PG = True
else:
    import sqlite3
    def get_conn():
        conn = sqlite3.connect('crm.db')
        conn.row_factory = sqlite3.Row
        return conn
    IS_PG = False

def ph(q):
    return q.replace('?', '%s') if IS_PG else q

def select(query, args=(), one=False):
    conn = get_conn()
    try:
        if IS_PG:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
        c.execute(ph(query), args)
        rows = c.fetchall()
        result = [dict(r) for r in rows]
        return (result[0] if result else None) if one else result
    finally:
        conn.close()

def execute(query, args=()):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(ph(query), args)
        conn.commit()
    finally:
        conn.close()

def insert(query, args=()):
    conn = get_conn()
    try:
        c = conn.cursor()
        if IS_PG:
            c.execute(ph(query + ' RETURNING id'), args)
            conn.commit()
            row = c.fetchone()
            return row[0] if row else None
        else:
            c.execute(query, args)
            conn.commit()
            return c.lastrowid
    finally:
        conn.close()

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def init_db():
    conn = get_conn()
    c = conn.cursor()
    serial = 'SERIAL' if IS_PG else 'INTEGER'
    auto = '' if IS_PG else 'AUTOINCREMENT'

    stmts = [
        f'''CREATE TABLE IF NOT EXISTS usuarios (
            id {serial} PRIMARY KEY {auto},
            nombre TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            rol TEXT DEFAULT 'vendedor',
            activo INTEGER DEFAULT 1,
            creado TEXT
        )''',
        f'''CREATE TABLE IF NOT EXISTS sesiones (
            token TEXT PRIMARY KEY,
            usuario_id INTEGER NOT NULL,
            creado TEXT
        )''',
        f'''CREATE TABLE IF NOT EXISTS empresas (
            id {serial} PRIMARY KEY {auto},
            nombre TEXT NOT NULL,
            industria TEXT,
            tamano TEXT,
            creado TEXT
        )''',
        f'''CREATE TABLE IF NOT EXISTS personas (
            id {serial} PRIMARY KEY {auto},
            nombre TEXT NOT NULL,
            email TEXT,
            telefono TEXT,
            cargo TEXT,
            empresa_id INTEGER,
            creado TEXT
        )''',
        f'''CREATE TABLE IF NOT EXISTS oportunidades (
            id {serial} PRIMARY KEY {auto},
            titulo TEXT NOT NULL,
            persona_id INTEGER,
            empresa_id INTEGER,
            usuario_id INTEGER,
            valor NUMERIC DEFAULT 0,
            etapa TEXT DEFAULT 'Prospecto',
            probabilidad INTEGER DEFAULT 5,
            fecha_cierre TEXT,
            notas TEXT,
            creado TEXT,
            actualizado TEXT
        )''',
        f'''CREATE TABLE IF NOT EXISTS tareas (
            id {serial} PRIMARY KEY {auto},
            oportunidad_id INTEGER,
            usuario_id INTEGER,
            descripcion TEXT NOT NULL,
            fecha_limite TEXT,
            completada INTEGER DEFAULT 0,
            creado TEXT
        )''',
        f'''CREATE TABLE IF NOT EXISTS actividades (
            id {serial} PRIMARY KEY {auto},
            oportunidad_id INTEGER,
            usuario_id INTEGER,
            tipo TEXT,
            descripcion TEXT,
            fecha TEXT
        )'''
    ]

    for s in stmts:
        c.execute(s)

    conn.commit()

    # Crear admin por defecto si no existe
    if IS_PG:
        c.execute("SELECT COUNT(*) FROM usuarios")
        count = c.fetchone()[0]
    else:
        conn2 = get_conn()
        conn2.row_factory = sqlite3.Row
        c2 = conn2.cursor()
        c2.execute("SELECT COUNT(*) as n FROM usuarios")
        count = c2.fetchone()['n']
        conn2.close()

    if count == 0:
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        c.execute(ph("INSERT INTO usuarios (nombre, email, password, rol, creado) VALUES (?,?,?,?,?)"),
                  ('Admin', 'admin@fillgap.com', hash_pw('fillgap2024'), 'admin', now))
        conn.commit()

    conn.close()

# Etapas y probabilidades por defecto
ETAPAS = {
    'Prospecto': 5,
    'Cita': 10,
    'Propuesta': 50,
    'Sí Verbal': 80,
    'Ganado': 100,
    'Perdido': 0
}

def get_user_from_token(token):
    if not token:
        return None
    s = select('SELECT * FROM sesiones WHERE token=?', (token,), one=True)
    if not s:
        return None
    return select('SELECT * FROM usuarios WHERE id=? AND activo=1', (s['usuario_id'],), one=True)


class CRMHandler(BaseHTTPRequestHandler):
    def log_message(self, f, *a): pass

    def get_token(self):
        auth = self.headers.get('Authorization', '')
        if auth.startswith('Bearer '):
            return auth[7:]
        return None

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, content):
        body = content.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()

    def require_auth(self):
        user = get_user_from_token(self.get_token())
        if not user:
            self.send_json({'error': 'No autorizado'}, 401)
        return user

    def read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def do_GET(self):
        path = self.path.split('?')[0]

        if path in ('/', '/index.html'):
            with open(os.path.join(os.path.dirname(__file__), 'index.html'), 'r', encoding='utf-8') as f:
                self.send_html(f.read())
            return

        # Auth check for API
        if path.startswith('/api/') and path != '/api/login':
            user = get_user_from_token(self.get_token())
            if not user:
                self.send_json({'error': 'No autorizado'}, 401)
                return
        else:
            user = get_user_from_token(self.get_token())

        now = datetime.now().strftime('%Y-%m-%d %H:%M')

        if path == '/api/me':
            self.send_json(user)

        elif path == '/api/etapas':
            self.send_json(ETAPAS)

        elif path == '/api/dashboard':
            total = select('SELECT COUNT(*) as n FROM oportunidades', one=True)['n']
            valor = select('SELECT SUM(valor) as v FROM oportunidades WHERE etapa NOT IN (?,?)', ('Perdido','Ganado'), one=True)['v'] or 0
            ganado = select("SELECT SUM(valor) as v FROM oportunidades WHERE etapa='Ganado'", one=True)['v'] or 0
            por_etapa = select('SELECT etapa, COUNT(*) as n, SUM(valor) as v FROM oportunidades GROUP BY etapa')
            por_vendedor = select('''SELECT u.nombre, COUNT(o.id) as ops, SUM(o.valor) as valor
                FROM usuarios u LEFT JOIN oportunidades o ON o.usuario_id=u.id
                WHERE u.activo=1 GROUP BY u.id, u.nombre ORDER BY valor DESC''')
            recientes = select('''SELECT o.*, u.nombre as vendedor, p.nombre as persona_nombre, e.nombre as empresa_nombre
                FROM oportunidades o
                LEFT JOIN usuarios u ON o.usuario_id=u.id
                LEFT JOIN personas p ON o.persona_id=p.id
                LEFT JOIN empresas e ON o.empresa_id=e.id
                ORDER BY o.actualizado DESC LIMIT 8''')
            tareas_pendientes = select('''SELECT t.*, o.titulo as op_titulo
                FROM tareas t LEFT JOIN oportunidades o ON t.oportunidad_id=o.id
                WHERE t.completada=0 ORDER BY t.fecha_limite ASC LIMIT 10''')
            self.send_json({
                'total': total, 'valor_pipeline': float(valor),
                'valor_ganado': float(ganado),
                'por_etapa': por_etapa, 'por_vendedor': por_vendedor,
                'recientes': recientes, 'tareas_pendientes': tareas_pendientes
            })

        elif path == '/api/oportunidades':
            ops = select('''SELECT o.*, u.nombre as vendedor, p.nombre as persona_nombre, e.nombre as empresa_nombre
                FROM oportunidades o
                LEFT JOIN usuarios u ON o.usuario_id=u.id
                LEFT JOIN personas p ON o.persona_id=p.id
                LEFT JOIN empresas e ON o.empresa_id=e.id
                ORDER BY o.actualizado DESC''')
            self.send_json(ops)

        elif path.startswith('/api/oportunidades/'):
            oid = path.split('/')[-1]
            op = select('''SELECT o.*, u.nombre as vendedor, p.nombre as persona_nombre,
                p.email as persona_email, p.telefono as persona_tel, p.cargo,
                e.nombre as empresa_nombre, e.industria
                FROM oportunidades o
                LEFT JOIN usuarios u ON o.usuario_id=u.id
                LEFT JOIN personas p ON o.persona_id=p.id
                LEFT JOIN empresas e ON o.empresa_id=e.id
                WHERE o.id=?''', (oid,), one=True)
            if op:
                op['tareas'] = select('SELECT t.*, u.nombre as asignado FROM tareas t LEFT JOIN usuarios u ON t.usuario_id=u.id WHERE t.oportunidad_id=? ORDER BY t.fecha_limite ASC', (oid,))
                op['actividades'] = select('SELECT a.*, u.nombre as usuario_nombre FROM actividades a LEFT JOIN usuarios u ON a.usuario_id=u.id WHERE a.oportunidad_id=? ORDER BY a.fecha DESC', (oid,))
                self.send_json(op)
            else:
                self.send_json({'error': 'No encontrado'}, 404)

        elif path == '/api/empresas':
            self.send_json(select('SELECT * FROM empresas ORDER BY nombre'))

        elif path.startswith('/api/empresas/'):
            eid = path.split('/')[-1]
            emp = select('SELECT * FROM empresas WHERE id=?', (eid,), one=True)
            if emp:
                emp['personas'] = select('SELECT * FROM personas WHERE empresa_id=?', (eid,))
                emp['oportunidades'] = select('''SELECT o.*, u.nombre as vendedor FROM oportunidades o
                    LEFT JOIN usuarios u ON o.usuario_id=u.id WHERE o.empresa_id=?''', (eid,))
                self.send_json(emp)
            else:
                self.send_json({'error': 'No encontrado'}, 404)

        elif path == '/api/personas':
            self.send_json(select('''SELECT p.*, e.nombre as empresa_nombre
                FROM personas p LEFT JOIN empresas e ON p.empresa_id=e.id ORDER BY p.nombre'''))

        elif path == '/api/usuarios':
            if user and user['rol'] == 'admin':
                self.send_json(select('SELECT id, nombre, email, rol, activo, creado FROM usuarios ORDER BY nombre'))
            else:
                self.send_json(select('SELECT id, nombre FROM usuarios WHERE activo=1 ORDER BY nombre'))

        elif path == '/api/tareas':
            self.send_json(select('''SELECT t.*, o.titulo as op_titulo, u.nombre as asignado
                FROM tareas t
                LEFT JOIN oportunidades o ON t.oportunidad_id=o.id
                LEFT JOIN usuarios u ON t.usuario_id=u.id
                WHERE t.completada=0 ORDER BY t.fecha_limite ASC'''))

        else:
            self.send_json({'error': 'No encontrado'}, 404)

    def do_POST(self):
        path = self.path.split('?')[0]
        body = self.read_body()
        now = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Login público
        if path == '/api/login':
            user = select('SELECT * FROM usuarios WHERE email=? AND activo=1', (body.get('email',''),), one=True)
            if user and user['password'] == hash_pw(body.get('password','')):
                token = secrets.token_hex(32)
                insert('INSERT INTO sesiones (token, usuario_id, creado) VALUES (?,?,?)',
                       (token, user['id'], now))
                self.send_json({'token': token, 'user': {
                    'id': user['id'], 'nombre': user['nombre'],
                    'email': user['email'], 'rol': user['rol']
                }})
            else:
                self.send_json({'error': 'Credenciales incorrectas'}, 401)
            return

        user = get_user_from_token(self.get_token())
        if not user:
            self.send_json({'error': 'No autorizado'}, 401)
            return

        if path == '/api/logout':
            execute('DELETE FROM sesiones WHERE token=?', (self.get_token(),))
            self.send_json({'ok': True})

        elif path == '/api/empresas':
            eid = insert('INSERT INTO empresas (nombre, industria, tamano, creado) VALUES (?,?,?,?)',
                        (body.get('nombre',''), body.get('industria',''), body.get('tamano',''), now))
            self.send_json({'id': eid, 'ok': True})

        elif path == '/api/personas':
            pid = insert('INSERT INTO personas (nombre, email, telefono, cargo, empresa_id, creado) VALUES (?,?,?,?,?,?)',
                        (body.get('nombre',''), body.get('email',''), body.get('telefono',''),
                         body.get('cargo',''), body.get('empresa_id'), now))
            self.send_json({'id': pid, 'ok': True})

        elif path == '/api/oportunidades':
            etapa = body.get('etapa', 'Prospecto')
            prob = body.get('probabilidad', ETAPAS.get(etapa, 5))
            oid = insert('''INSERT INTO oportunidades
                (titulo, persona_id, empresa_id, usuario_id, valor, etapa, probabilidad, fecha_cierre, notas, creado, actualizado)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (body.get('titulo',''), body.get('persona_id'), body.get('empresa_id'),
                 body.get('usuario_id', user['id']), body.get('valor', 0),
                 etapa, prob, body.get('fecha_cierre',''), body.get('notas',''), now, now))
            # Registrar actividad
            insert('INSERT INTO actividades (oportunidad_id, usuario_id, tipo, descripcion, fecha) VALUES (?,?,?,?,?)',
                   (oid, user['id'], 'Sistema', f"Oportunidad creada por {user['nombre']}", now))
            self.send_json({'id': oid, 'ok': True})

        elif path == '/api/tareas':
            tid = insert('INSERT INTO tareas (oportunidad_id, usuario_id, descripcion, fecha_limite, creado) VALUES (?,?,?,?,?)',
                        (body.get('oportunidad_id'), body.get('usuario_id', user['id']),
                         body.get('descripcion',''), body.get('fecha_limite',''), now))
            execute('UPDATE oportunidades SET actualizado=? WHERE id=?', (now, body.get('oportunidad_id')))
            self.send_json({'id': tid, 'ok': True})

        elif path == '/api/actividades':
            aid = insert('INSERT INTO actividades (oportunidad_id, usuario_id, tipo, descripcion, fecha) VALUES (?,?,?,?,?)',
                        (body.get('oportunidad_id'), user['id'], body.get('tipo','Nota'), body.get('descripcion',''), now))
            execute('UPDATE oportunidades SET actualizado=? WHERE id=?', (now, body.get('oportunidad_id')))
            self.send_json({'id': aid, 'ok': True})

        elif path == '/api/usuarios' and user['rol'] == 'admin':
            uid = insert('INSERT INTO usuarios (nombre, email, password, rol, creado) VALUES (?,?,?,?,?)',
                        (body.get('nombre',''), body.get('email',''),
                         hash_pw(body.get('password','fillgap2024')), body.get('rol','vendedor'), now))
            self.send_json({'id': uid, 'ok': True})

        else:
            self.send_json({'error': 'No encontrado'}, 404)

    def do_PUT(self):
        path = self.path.split('?')[0]
        body = self.read_body()
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        user = get_user_from_token(self.get_token())
        if not user:
            self.send_json({'error': 'No autorizado'}, 401)
            return

        if path.startswith('/api/oportunidades/'):
            oid = path.split('/')[-1]
            etapa = body.get('etapa', 'Prospecto')
            # Auto-probabilidad si no viene manual
            prob = body.get('probabilidad', ETAPAS.get(etapa, 5))
            old = select('SELECT etapa FROM oportunidades WHERE id=?', (oid,), one=True)
            execute('''UPDATE oportunidades SET titulo=?, persona_id=?, empresa_id=?, usuario_id=?,
                valor=?, etapa=?, probabilidad=?, fecha_cierre=?, notas=?, actualizado=? WHERE id=?''',
                (body.get('titulo'), body.get('persona_id'), body.get('empresa_id'),
                 body.get('usuario_id'), body.get('valor'), etapa, prob,
                 body.get('fecha_cierre'), body.get('notas'), now, oid))
            if old and old['etapa'] != etapa:
                insert('INSERT INTO actividades (oportunidad_id, usuario_id, tipo, descripcion, fecha) VALUES (?,?,?,?,?)',
                       (oid, user['id'], 'Sistema', f"Etapa cambiada de {old['etapa']} a {etapa} por {user['nombre']}", now))
            self.send_json({'ok': True})

        elif path.startswith('/api/tareas/'):
            tid = path.split('/')[-1]
            execute('UPDATE tareas SET descripcion=?, fecha_limite=?, completada=?, usuario_id=? WHERE id=?',
                    (body.get('descripcion'), body.get('fecha_limite'), body.get('completada', 0),
                     body.get('usuario_id'), tid))
            self.send_json({'ok': True})

        elif path.startswith('/api/empresas/'):
            eid = path.split('/')[-1]
            execute('UPDATE empresas SET nombre=?, industria=?, tamano=? WHERE id=?',
                    (body.get('nombre'), body.get('industria'), body.get('tamano'), eid))
            self.send_json({'ok': True})

        elif path.startswith('/api/personas/'):
            pid = path.split('/')[-1]
            execute('UPDATE personas SET nombre=?, email=?, telefono=?, cargo=?, empresa_id=? WHERE id=?',
                    (body.get('nombre'), body.get('email'), body.get('telefono'),
                     body.get('cargo'), body.get('empresa_id'), pid))
            self.send_json({'ok': True})

        elif path.startswith('/api/usuarios/') and user['rol'] == 'admin':
            uid = path.split('/')[-1]
            if body.get('password'):
                execute('UPDATE usuarios SET nombre=?, email=?, rol=?, activo=?, password=? WHERE id=?',
                        (body.get('nombre'), body.get('email'), body.get('rol'),
                         body.get('activo', 1), hash_pw(body['password']), uid))
            else:
                execute('UPDATE usuarios SET nombre=?, email=?, rol=?, activo=? WHERE id=?',
                        (body.get('nombre'), body.get('email'), body.get('rol'), body.get('activo', 1), uid))
            self.send_json({'ok': True})

        else:
            self.send_json({'error': 'No encontrado'}, 404)

    def do_DELETE(self):
        path = self.path.split('?')[0]
        user = get_user_from_token(self.get_token())
        if not user:
            self.send_json({'error': 'No autorizado'}, 401)
            return

        if path.startswith('/api/oportunidades/'):
            oid = path.split('/')[-1]
            execute('DELETE FROM tareas WHERE oportunidad_id=?', (oid,))
            execute('DELETE FROM actividades WHERE oportunidad_id=?', (oid,))
            execute('DELETE FROM oportunidades WHERE id=?', (oid,))
            self.send_json({'ok': True})

        elif path.startswith('/api/tareas/'):
            tid = path.split('/')[-1]
            execute('DELETE FROM tareas WHERE id=?', (tid,))
            self.send_json({'ok': True})

        elif path.startswith('/api/empresas/') and user['rol'] == 'admin':
            eid = path.split('/')[-1]
            execute('DELETE FROM empresas WHERE id=?', (eid,))
            self.send_json({'ok': True})

        else:
            self.send_json({'error': 'No encontrado'}, 404)


if __name__ == '__main__':
    init_db()
    PORT = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', PORT), CRMHandler)
    print(f"FillGap CRM corriendo en puerto {PORT}")
    print(f"Usuario admin: admin@fillgap.com / fillgap2024")
    server.serve_forever()

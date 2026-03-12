#!/usr/bin/env python3
"""
CRM Server - Ventas y Oportunidades
Versión Railway / PostgreSQL
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
from datetime import datetime

# Soporte PostgreSQL (Railway) y SQLite (local)
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras
    def get_conn():
        return psycopg2.connect(DATABASE_URL)
    PLACEHOLDER = '%s'
else:
    import sqlite3
    def get_conn():
        conn = sqlite3.connect('crm.db')
        conn.row_factory = sqlite3.Row
        return conn
    PLACEHOLDER = '?'

def ph(query):
    """Reemplaza ? por %s según la base de datos activa."""
    if DATABASE_URL:
        return query.replace('?', '%s')
    return query

def init_db():
    conn = get_conn()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute('''CREATE TABLE IF NOT EXISTS clientes (
            id SERIAL PRIMARY KEY,
            nombre TEXT NOT NULL,
            empresa TEXT,
            email TEXT,
            telefono TEXT,
            creado TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS oportunidades (
            id SERIAL PRIMARY KEY,
            cliente_id INTEGER,
            titulo TEXT NOT NULL,
            valor NUMERIC,
            etapa TEXT DEFAULT 'Prospecto',
            probabilidad INTEGER DEFAULT 10,
            fecha_cierre TEXT,
            notas TEXT,
            creado TEXT,
            actualizado TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS actividades (
            id SERIAL PRIMARY KEY,
            oportunidad_id INTEGER,
            tipo TEXT,
            descripcion TEXT,
            fecha TEXT
        )''')
    else:
        c.execute('''CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            empresa TEXT,
            email TEXT,
            telefono TEXT,
            creado TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS oportunidades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER,
            titulo TEXT NOT NULL,
            valor REAL,
            etapa TEXT DEFAULT 'Prospecto',
            probabilidad INTEGER DEFAULT 10,
            fecha_cierre TEXT,
            notas TEXT,
            creado TEXT,
            actualizado TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS actividades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            oportunidad_id INTEGER,
            tipo TEXT,
            descripcion TEXT,
            fecha TEXT
        )''')
    conn.commit()
    conn.close()

def query_db(query, args=(), one=False, write=False):
    conn = get_conn()
    if DATABASE_URL:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        c = conn.cursor()
    c.execute(ph(query), args)
    if write:
        conn.commit()
        if DATABASE_URL:
            # Para PostgreSQL, obtener el ID insertado
            try:
                lastid = c.fetchone()
                lastid = lastid['id'] if lastid else None
            except:
                lastid = None
        else:
            lastid = c.lastrowid
        conn.close()
        return lastid
    rows = c.fetchall()
    conn.close()
    if DATABASE_URL:
        result = [dict(row) for row in rows]
    else:
        result = [dict(zip([d[0] for d in c.description], row)) for row in rows] if rows else []
        # Re-fetch since cursor is closed
        conn2 = get_conn()
        conn2.row_factory = sqlite3.Row
        c2 = conn2.cursor()
        c2.execute(ph(query), args)
        rows2 = c2.fetchall()
        conn2.close()
        result = [dict(row) for row in rows2]
    return (result[0] if result else None) if one else result

def query_db_safe(query, args=(), one=False, write=False):
    """Versión segura que maneja la conexión correctamente."""
    conn = get_conn()
    try:
        if DATABASE_URL:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
        
        c.execute(ph(query), args)
        
        if write:
            if DATABASE_URL and 'RETURNING' not in query:
                pass
            conn.commit()
            if DATABASE_URL:
                try:
                    row = c.fetchone()
                    lastid = row['id'] if row else None
                except:
                    lastid = None
            else:
                lastid = c.lastrowid
            return lastid
        
        rows = c.fetchall()
        if DATABASE_URL:
            result = [dict(row) for row in rows]
        else:
            result = [dict(row) for row in rows]
        return (result[0] if result else None) if one else result
    finally:
        conn.close()

def insert(query, args=()):
    """Insert y retorna el ID."""
    if DATABASE_URL:
        q = query + ' RETURNING id'
    else:
        q = query
    return query_db_safe(q, args, write=True)

def execute(query, args=()):
    """Ejecuta UPDATE o DELETE."""
    conn = get_conn()
    try:
        if DATABASE_URL:
            c = conn.cursor()
        else:
            c = conn.cursor()
        c.execute(ph(query), args)
        conn.commit()
    finally:
        conn.close()

def select(query, args=(), one=False):
    """SELECT y retorna filas."""
    conn = get_conn()
    try:
        if DATABASE_URL:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
        c.execute(ph(query), args)
        rows = c.fetchall()
        result = [dict(row) for row in rows]
        return (result[0] if result else None) if one else result
    finally:
        conn.close()


class CRMHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
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
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        path = self.path.split('?')[0]

        if path == '/' or path == '/index.html':
            html_path = os.path.join(os.path.dirname(__file__), 'index.html')
            with open(html_path, 'r', encoding='utf-8') as f:
                self.send_html(f.read())

        elif path == '/api/dashboard':
            total = select('SELECT COUNT(*) as n FROM oportunidades', one=True)
            valor = select('SELECT SUM(valor) as v FROM oportunidades', one=True)
            por_etapa = select('SELECT etapa, COUNT(*) as n, SUM(valor) as v FROM oportunidades GROUP BY etapa')
            recientes = select('''SELECT o.*, c.nombre as cliente_nombre, c.empresa 
                FROM oportunidades o LEFT JOIN clientes c ON o.cliente_id=c.id 
                ORDER BY o.actualizado DESC LIMIT 5''')
            self.send_json({
                'total_oportunidades': total['n'] if total else 0,
                'valor_total': float(valor['v']) if valor and valor['v'] else 0,
                'por_etapa': por_etapa,
                'recientes': recientes
            })

        elif path == '/api/oportunidades' or path == '/api/oportunidades/':
            ops = select('''SELECT o.*, c.nombre as cliente_nombre, c.empresa 
                FROM oportunidades o LEFT JOIN clientes c ON o.cliente_id=c.id 
                ORDER BY o.actualizado DESC''')
            self.send_json(ops)

        elif path.startswith('/api/oportunidades/'):
            oid = path.split('/')[-1]
            op = select('''SELECT o.*, c.nombre as cliente_nombre, c.empresa, c.email, c.telefono
                FROM oportunidades o LEFT JOIN clientes c ON o.cliente_id=c.id 
                WHERE o.id=?''', (oid,), one=True)
            if op:
                acts = select('SELECT * FROM actividades WHERE oportunidad_id=? ORDER BY fecha DESC', (oid,))
                op['actividades'] = acts
                self.send_json(op)
            else:
                self.send_json({'error': 'No encontrado'}, 404)

        elif path == '/api/clientes':
            clientes = select('SELECT * FROM clientes ORDER BY nombre')
            self.send_json(clientes)

        else:
            self.send_json({'error': 'Ruta no encontrada'}, 404)

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        path = self.path.split('?')[0]
        now = datetime.now().strftime('%Y-%m-%d %H:%M')

        if path == '/api/clientes':
            lid = insert(
                'INSERT INTO clientes (nombre, empresa, email, telefono, creado) VALUES (?,?,?,?,?)',
                (body.get('nombre',''), body.get('empresa',''), body.get('email',''), body.get('telefono',''), now)
            )
            self.send_json({'id': lid, 'ok': True})

        elif path == '/api/oportunidades':
            cliente_id = body.get('cliente_id')
            if not cliente_id and body.get('cliente_nombre'):
                cliente_id = insert(
                    'INSERT INTO clientes (nombre, empresa, email, telefono, creado) VALUES (?,?,?,?,?)',
                    (body['cliente_nombre'], body.get('empresa',''), body.get('email',''), body.get('telefono',''), now)
                )
            oid = insert(
                '''INSERT INTO oportunidades (cliente_id, titulo, valor, etapa, probabilidad, fecha_cierre, notas, creado, actualizado)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (cliente_id, body.get('titulo',''), body.get('valor',0), body.get('etapa','Prospecto'),
                 body.get('probabilidad',10), body.get('fecha_cierre',''), body.get('notas',''), now, now)
            )
            self.send_json({'id': oid, 'ok': True})

        elif path == '/api/actividades':
            aid = insert(
                'INSERT INTO actividades (oportunidad_id, tipo, descripcion, fecha) VALUES (?,?,?,?)',
                (body.get('oportunidad_id'), body.get('tipo','Nota'), body.get('descripcion',''), now)
            )
            execute('UPDATE oportunidades SET actualizado=? WHERE id=?',
                    (now, body.get('oportunidad_id')))
            self.send_json({'id': aid, 'ok': True})

        else:
            self.send_json({'error': 'Ruta no encontrada'}, 404)

    def do_PUT(self):
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        path = self.path.split('?')[0]
        now = datetime.now().strftime('%Y-%m-%d %H:%M')

        if path.startswith('/api/oportunidades/'):
            oid = path.split('/')[-1]
            execute(
                '''UPDATE oportunidades SET titulo=?, valor=?, etapa=?, probabilidad=?, 
                   fecha_cierre=?, notas=?, actualizado=? WHERE id=?''',
                (body.get('titulo'), body.get('valor'), body.get('etapa'), body.get('probabilidad'),
                 body.get('fecha_cierre'), body.get('notas'), now, oid)
            )
            self.send_json({'ok': True})

        elif path.startswith('/api/clientes/'):
            cid = path.split('/')[-1]
            execute(
                'UPDATE clientes SET nombre=?, empresa=?, email=?, telefono=? WHERE id=?',
                (body.get('nombre'), body.get('empresa'), body.get('email'), body.get('telefono'), cid)
            )
            self.send_json({'ok': True})

    def do_DELETE(self):
        path = self.path.split('?')[0]
        if path.startswith('/api/oportunidades/'):
            oid = path.split('/')[-1]
            execute('DELETE FROM actividades WHERE oportunidad_id=?', (oid,))
            execute('DELETE FROM oportunidades WHERE id=?', (oid,))
            self.send_json({'ok': True})

if __name__ == '__main__':
    init_db()
    PORT = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', PORT), CRMHandler)
    print(f"CRM corriendo en puerto {PORT}")
    server.serve_forever()

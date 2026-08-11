use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use common::FiredAlert;

pub type DbConn = Arc<Mutex<Connection>>;

pub fn init_db(path: &str) -> rusqlite::Result<DbConn> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS alerts (
            id           TEXT PRIMARY KEY,
            broker_id    TEXT NOT NULL,
            metric       TEXT NOT NULL,
            operator     TEXT NOT NULL,
            value        REAL NOT NULL,
            threshold    REAL NOT NULL,
            fired_at     INTEGER NOT NULL,
            acknowledged INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_fired_at ON alerts(fired_at);"
    )?;
    Ok(Arc::new(Mutex::new(conn)))
}

pub fn insert_alert(db: &DbConn, alert: &FiredAlert) -> rusqlite::Result<()> {
    db.lock().unwrap().execute(
        "INSERT INTO alerts (id, broker_id, metric, operator, value, threshold, fired_at, acknowledged)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            alert.id, alert.broker_id, alert.metric, alert.operator,
            alert.value, alert.threshold, alert.fired_at, alert.acknowledged as i32,
        ],
    )?;
    Ok(())
}

pub fn acknowledge_alert(db: &DbConn, id: &str) -> rusqlite::Result<bool> {
    let updated = db.lock().unwrap().execute(
        "UPDATE alerts SET acknowledged = 1 WHERE id = ?1",
        rusqlite::params![id],
    )?;
    Ok(updated > 0)
}

pub fn load_all_alerts(db: &DbConn) -> rusqlite::Result<Vec<FiredAlert>> {
    let conn = db.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, broker_id, metric, operator, value, threshold, fired_at, acknowledged
         FROM alerts ORDER BY fired_at ASC"
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(FiredAlert {
            id: row.get(0)?,
            broker_id: row.get(1)?,
            metric: row.get(2)?,
            operator: row.get(3)?,
            value: row.get(4)?,
            threshold: row.get(5)?,
            fired_at: row.get(6)?,
            acknowledged: row.get::<_, i32>(7)? != 0,
        })
    })?;
    rows.collect()
}
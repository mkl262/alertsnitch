package sqlstore

import (
	"context"
	"crypto/md5" //nolint:gosec // KvHash dedup digest; must match database/*/0.2.0-labelkv.sql
	"database/sql"
	"encoding/hex"
)

// kvPairHash is a fixed-width digest of key || ASCII SOH || value for unique indexes (MySQL index size limit; PG rejects NUL in text).
func kvPairHash(k, v string) string {
	sum := md5.Sum([]byte(k + "\x01" + v)) //nolint:gosec // not for security; fixed-width unique-index key
	return hex.EncodeToString(sum[:])
}

func postgresGetLabelKVID(ctx context.Context, tx *sql.Tx, k, v string) (int64, error) {
	h := kvPairHash(k, v)
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO LabelKV (LabelKey, Value, KvHash) VALUES ($1, $2, $3)
		ON CONFLICT (KvHash) DO UPDATE SET LabelKey = LabelKV.LabelKey
		RETURNING ID`, k, v, h).Scan(&id)
	return id, err
}

func postgresGetAnnotationKVID(ctx context.Context, tx *sql.Tx, k, v string) (int64, error) {
	h := kvPairHash(k, v)
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO AnnotationKV (AnnotationKey, Value, KvHash) VALUES ($1, $2, $3)
		ON CONFLICT (KvHash) DO UPDATE SET AnnotationKey = AnnotationKV.AnnotationKey
		RETURNING ID`, k, v, h).Scan(&id)
	return id, err
}

func mysqlGetLabelKVID(ctx context.Context, tx *sql.Tx, k, v string) (int64, error) {
	h := kvPairHash(k, v)
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO LabelKV (LabelKey, Value, KvHash) VALUES (?, ?, ?)`, k, v, h); err != nil {
		return 0, err
	}
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT ID FROM LabelKV WHERE KvHash = ?`, h).Scan(&id)
	return id, err
}

func mysqlGetAnnotationKVID(ctx context.Context, tx *sql.Tx, k, v string) (int64, error) {
	h := kvPairHash(k, v)
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO AnnotationKV (AnnotationKey, Value, KvHash) VALUES (?, ?, ?)`, k, v, h); err != nil {
		return 0, err
	}
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT ID FROM AnnotationKV WHERE KvHash = ?`, h).Scan(&id)
	return id, err
}

func postgresGetReceiverID(ctx context.Context, tx *sql.Tx, receiver string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO AlertGroupReceiver (Receiver) VALUES ($1)
		ON CONFLICT (Receiver) DO UPDATE SET Receiver = AlertGroupReceiver.Receiver
		RETURNING ID`, receiver).Scan(&id)
	return id, err
}

func postgresGetExternalURLID(ctx context.Context, tx *sql.Tx, externalURL string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO AlertGroupExternalURL (ExternalURL) VALUES ($1)
		ON CONFLICT (ExternalURL) DO UPDATE SET ExternalURL = AlertGroupExternalURL.ExternalURL
		RETURNING ID`, externalURL).Scan(&id)
	return id, err
}

func postgresGetKeyID(ctx context.Context, tx *sql.Tx, groupKey string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO AlertGroupKey (GroupKey) VALUES ($1)
		ON CONFLICT (GroupKey) DO UPDATE SET GroupKey = AlertGroupKey.GroupKey
		RETURNING ID`, groupKey).Scan(&id)
	return id, err
}

func mysqlGetReceiverID(ctx context.Context, tx *sql.Tx, receiver string) (int64, error) {
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO AlertGroupReceiver (Receiver) VALUES (?)`, receiver); err != nil {
		return 0, err
	}
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT ID FROM AlertGroupReceiver WHERE Receiver = ?`, receiver).Scan(&id)
	return id, err
}

func mysqlGetExternalURLID(ctx context.Context, tx *sql.Tx, externalURL string) (int64, error) {
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO AlertGroupExternalURL (ExternalURL) VALUES (?)`, externalURL); err != nil {
		return 0, err
	}
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT ID FROM AlertGroupExternalURL WHERE ExternalURL = ?`, externalURL).Scan(&id)
	return id, err
}

func mysqlGetKeyID(ctx context.Context, tx *sql.Tx, groupKey string) (int64, error) {
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO AlertGroupKey (GroupKey) VALUES (?)`, groupKey); err != nil {
		return 0, err
	}
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT ID FROM AlertGroupKey WHERE GroupKey = ?`, groupKey).Scan(&id)
	return id, err
}

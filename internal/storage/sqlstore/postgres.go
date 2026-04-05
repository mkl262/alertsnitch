package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mikehsu0618/alertsnitch/internal"
	"github.com/mikehsu0618/alertsnitch/internal/metrics"
)

// Postgres is the PostgreSQL storage backend.
type Postgres struct {
	base
}

// ConnectPostgres opens a Postgres backend and verifies its model.
func ConnectPostgres(cfg Config) (*Postgres, error) {
	b, err := open("postgres", cfg)
	if err != nil {
		return nil, err
	}
	db := &Postgres{base: b}
	if err := db.verifyOnConnect(); err != nil {
		return nil, err
	}
	return db, nil
}

// Save persists an alert group. extraLabels is ignored by SQL backends.
func (d *Postgres) Save(ctx context.Context, data *internal.AlertGroup, _ map[string]string) error {
	err := d.unitOfWork(ctx, func(tx *sql.Tx) error {
		var alertGroupID int64
		err := tx.QueryRowContext(ctx, `
			INSERT INTO AlertGroup (time, receiver, status, externalURL, groupKey)
			VALUES (current_timestamp, $1, $2, $3, $4) RETURNING ID`,
			data.Receiver, data.Status, data.ExternalURL, data.GroupKey).Scan(&alertGroupID)
		if err != nil {
			return fmt.Errorf("failed to insert into AlertGroups: %w", err)
		}

		if err := insertGroupLabelsPostgres(ctx, tx, alertGroupID, data.GroupLabels); err != nil {
			return err
		}
		if err := insertCommonLabelsPostgres(ctx, tx, alertGroupID, data.CommonLabels); err != nil {
			return err
		}
		if err := insertCommonAnnotationsPostgres(ctx, tx, alertGroupID, data.CommonAnnotations); err != nil {
			return err
		}

		return insertAlertsPostgres(ctx, tx, alertGroupID, data.Alerts)
	})
	metrics.RecordSaveOutcome(data.Receiver, data.Status, len(data.Alerts), err)
	return err
}

func (*Postgres) String() string { return "postgres database driver" }

func insertAlertsPostgres(ctx context.Context, tx *sql.Tx, alertGroupID int64, alerts []internal.Alert) error {
	for _, alert := range alerts {
		var row *sql.Row
		if alert.EndsAt.Before(alert.StartsAt) {
			row = tx.QueryRowContext(ctx, `
			INSERT INTO Alert (alertGroupID, status, startsAt, generatorURL, fingerprint)
			VALUES ($1, $2, $3, $4, $5) RETURNING ID`,
				alertGroupID, alert.Status, alert.StartsAt, alert.GeneratorURL, alert.Fingerprint)
		} else {
			row = tx.QueryRowContext(ctx, `
			INSERT INTO Alert (alertGroupID, status, startsAt, endsAt, generatorURL, fingerprint)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING ID`,
				alertGroupID, alert.Status, alert.StartsAt, alert.EndsAt, alert.GeneratorURL, alert.Fingerprint)
		}
		var alertID int64
		if err := row.Scan(&alertID); err != nil {
			return fmt.Errorf("failed to insert into Alert: %w", err)
		}

		if err := insertAlertLabelsPostgres(ctx, tx, alertID, alert.Labels); err != nil {
			return err
		}
		if err := insertAlertAnnotationsPostgres(ctx, tx, alertID, alert.Annotations); err != nil {
			return err
		}
	}
	return nil
}

func insertGroupLabelsPostgres(ctx context.Context, tx *sql.Tx, alertGroupID int64, kv map[string]string) error {
	for k, v := range kv {
		kvID, err := postgresGetLabelKVID(ctx, tx, k, v)
		if err != nil {
			return fmt.Errorf("failed to resolve GroupLabel LabelKV: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO GroupLabel (alertGroupID, LabelKVID)
			VALUES ($1, $2)`, alertGroupID, kvID); err != nil {
			return fmt.Errorf("failed to insert into GroupLabel: %w", err)
		}
	}
	return nil
}

func insertCommonLabelsPostgres(ctx context.Context, tx *sql.Tx, alertGroupID int64, kv map[string]string) error {
	for k, v := range kv {
		kvID, err := postgresGetLabelKVID(ctx, tx, k, v)
		if err != nil {
			return fmt.Errorf("failed to resolve CommonLabel LabelKV: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO CommonLabel (alertGroupID, LabelKVID)
			VALUES ($1, $2)`, alertGroupID, kvID); err != nil {
			return fmt.Errorf("failed to insert into CommonLabel: %w", err)
		}
	}
	return nil
}

func insertCommonAnnotationsPostgres(ctx context.Context, tx *sql.Tx, alertGroupID int64, kv map[string]string) error {
	for k, v := range kv {
		kvID, err := postgresGetAnnotationKVID(ctx, tx, k, v)
		if err != nil {
			return fmt.Errorf("failed to resolve CommonAnnotation AnnotationKV: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO CommonAnnotation (alertGroupID, AnnotationKVID)
			VALUES ($1, $2)`, alertGroupID, kvID); err != nil {
			return fmt.Errorf("failed to insert into CommonAnnotation: %w", err)
		}
	}
	return nil
}

func insertAlertLabelsPostgres(ctx context.Context, tx *sql.Tx, alertID int64, kv map[string]string) error {
	for k, v := range kv {
		kvID, err := postgresGetLabelKVID(ctx, tx, k, v)
		if err != nil {
			return fmt.Errorf("failed to resolve AlertLabel LabelKV: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO AlertLabel (AlertID, LabelKVID)
			VALUES ($1, $2)`, alertID, kvID); err != nil {
			return fmt.Errorf("failed to insert into AlertLabel: %w", err)
		}
	}
	return nil
}

func insertAlertAnnotationsPostgres(ctx context.Context, tx *sql.Tx, alertID int64, kv map[string]string) error {
	for k, v := range kv {
		kvID, err := postgresGetAnnotationKVID(ctx, tx, k, v)
		if err != nil {
			return fmt.Errorf("failed to resolve AlertAnnotation AnnotationKV: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO AlertAnnotation (AlertID, AnnotationKVID)
			VALUES ($1, $2)`, alertID, kvID); err != nil {
			return fmt.Errorf("failed to insert into AlertAnnotation: %w", err)
		}
	}
	return nil
}

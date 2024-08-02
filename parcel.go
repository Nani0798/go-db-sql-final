package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	const query = "INSERT INTO parcel (client, status, address, created_at) VALUES(?, ?, ?, ?) returning number;"

	var id int
	if err := s.db.QueryRow(query, p.Client, p.Status, p.Address, p.CreatedAt).Scan(&id); err != nil {
		return 0, fmt.Errorf("query row: %w", err)
	}

	return id, nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	const query = "select number, client, status, address, created_at from parcel where number = ?"

	p := Parcel{}
	if err := s.db.QueryRow(query, number).Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
		return Parcel{}, fmt.Errorf("query row: %w", err)
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	const query = "select number, client, status, address, created_at from parcel where client = ?"

	rows, err := s.db.Query(query, client)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var list []Parcel
	for rows.Next() {
		p := Parcel{}
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		list = append(list, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows read: %w", err)
	}

	return list, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	const query = "update parcel set status = ? where number = ?"

	if _, err := s.db.Exec(query, status, number); err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	const query = "update parcel set address = ? where status = 'registered' and number = ?"

	if _, err := s.db.Exec(query, address, number); err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	const query = "delete from parcel where status = 'registered' and number = ?"

	if _, err := s.db.Exec(query, number); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	
	return nil
}

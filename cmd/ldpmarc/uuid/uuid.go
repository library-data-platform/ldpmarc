package uuid

import (
	"github.com/jackc/pgtype"
)

const NilUUID string = "00000000-0000-0000-0000-000000000000"

func EncodeNilUUID() pgtype.UUID {
	u, err := EncodeUUID(NilUUID)
	if err != nil {
		panic("error encoding nil UUID")
	}
	return u
}

func EncodeUUID(uuid string) (pgtype.UUID, error) {
	var u pgtype.UUID
	err := u.Set(uuid)
	if err != nil {
		return pgtype.UUID{}, err
	}
	return u, nil
}


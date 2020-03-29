package user

import "encoding/json"

// User ...
type User struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
}

// NewUser ...
func NewUser(data []string) *User {
	return &User{FirstName: data[0], LastName: data[1], Email: data[2]}
}

// JSON ...returns Marshalled JSON bytes
func (user *User) JSON() []byte {
	body, err := json.Marshal(&user)
	if err != nil {
		panic(err)
	}
	return body
}

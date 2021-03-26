# Go drill client

`examples`

```lang=go
package main

func main() {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(t, err)
	}

	respBody, err := cl.UpsertView(d)
	if err != nil {
		t.Error(t, err)
	}

	assert.Equal(t, respBody.QueryState, "COMPLETED")
}
```
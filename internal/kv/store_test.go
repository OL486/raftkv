package kv

import (
	"encoding/json"
	"testing"
)

func TestStore_PutGetDelete(t *testing.T) {
	s := NewStore()
	if _, ok := s.Get("a"); ok {
		t.Fatal("空状态机不应有 key")
	}
	if _, err := s.Apply(EncodePut("a", "1")); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q %v, want 1 true", v, ok)
	}
	if _, err := s.Apply(EncodePut("a", "2")); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("a"); v != "2" {
		t.Fatalf("覆盖后 Get(a) = %q, want 2", v)
	}
	if _, err := s.Apply(EncodeDelete("a")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("删除后 Get(a) 应不存在")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

// TestStore_ReadCommand：线性化读是 no-op，只返回应用那一刻的值，
// 不改数据；读不存在的 key 返回 found=false。
func TestStore_ReadCommand(t *testing.T) {
	s := NewStore()
	res, err := s.Apply(EncodeRead("a"))
	if err != nil {
		t.Fatal(err)
	}
	var r ApplyResult
	if err := json.Unmarshal(res, &r); err != nil || r.Found {
		t.Fatalf("读不存在的 key 应 found=false，实际 %q %v", res, err)
	}
	if _, err := s.Apply(EncodePut("a", "7")); err != nil {
		t.Fatal(err)
	}
	res, err = s.Apply(EncodeRead("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(res, &r); err != nil || !r.Found || r.Value != "7" {
		t.Fatalf("读已存在的 key 应 found=true value=7，实际 %q %v", res, err)
	}
	if s.Len() != 1 {
		t.Fatalf("读不应改数据，Len = %d, want 1", s.Len())
	}
}

func TestStore_ApplyErrors(t *testing.T) {
	s := NewStore()
	if _, err := s.Apply([]byte("not json")); err == nil {
		t.Fatal("非法 JSON 命令应报错")
	}
	if _, err := s.Apply([]byte(`{"op":"wat","key":"a"}`)); err == nil {
		t.Fatal("未知 op 应报错")
	}
	if _, err := DecodeCommand([]byte("x")); err == nil {
		t.Fatal("DecodeCommand 对非法 JSON 应报错")
	}
}

func TestCommand_RoundTrip(t *testing.T) {
	c, err := DecodeCommand(EncodePut("k", "v"))
	if err != nil || c.Op != "put" || c.Key != "k" || c.Value != "v" {
		t.Fatalf("Put 编解码不对：%+v %v", c, err)
	}
	c, err = DecodeCommand(EncodeDelete("k"))
	if err != nil || c.Op != "delete" || c.Key != "k" {
		t.Fatalf("Delete 编解码不对：%+v %v", c, err)
	}
}

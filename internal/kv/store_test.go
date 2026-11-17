package kv

import "testing"

func TestStore_PutGetDelete(t *testing.T) {
	s := NewStore()
	if _, ok := s.Get("a"); ok {
		t.Fatal("空状态机不应有 key")
	}
	if err := s.Apply(EncodePut("a", "1")); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q %v, want 1 true", v, ok)
	}
	if err := s.Apply(EncodePut("a", "2")); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("a"); v != "2" {
		t.Fatalf("覆盖后 Get(a) = %q, want 2", v)
	}
	if err := s.Apply(EncodeDelete("a")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("删除后 Get(a) 应不存在")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

func TestStore_ApplyErrors(t *testing.T) {
	s := NewStore()
	if err := s.Apply([]byte("not json")); err == nil {
		t.Fatal("非法 JSON 命令应报错")
	}
	if err := s.Apply([]byte(`{"op":"wat","key":"a"}`)); err == nil {
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

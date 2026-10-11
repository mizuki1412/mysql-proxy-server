package logic

import "testing"

func TestScanPlaceholdersReplace(t *testing.T) {
	cases := []struct {
		in   string
		want string
		n    int
	}{
		{"select * from t where a=?", "select * from t where a=$1", 1},
		{"select ?+?", "select $1+$2", 2},
		{"select * from t where a=? and b='x?y'", "select * from t where a=$1 and b='x?y'", 1},
		{"select * from t where name='it''s ?' and id=?", "select * from t where name='it''s ?' and id=$1", 1},
		{`select * from t where name='it\'s ?' and id=?`, `select * from t where name='it\'s ?' and id=$1`, 1},
		{`select * from t where name="a?b" and id=?`, `select * from t where name="a?b" and id=$1`, 1},
		{"select `a?b` from t where id=?", "select `a?b` from t where id=$1", 1},
		{"-- comment ?\nselect * from t where id=?", "-- comment ?\nselect * from t where id=$1", 1},
		{"select 1 # comment ?\n, ?+1", "select 1 # comment ?\n, $1+1", 1},
		{"/* ? */ select ?", "/* ? */ select $1", 1},
		{"select * from t where note like '%?%'", "select * from t where note like '%?%'", 0},
	}
	for _, c := range cases {
		got, n := scanPlaceholders(c.in, func(k int) string {
			return "$" + string(rune('0'+k))
		})
		if got != c.want || n != c.n {
			t.Errorf("scanPlaceholders(%q) = (%q, %d), want (%q, %d)", c.in, got, n, c.want, c.n)
		}
	}
}

func TestCountPlaceholders(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"select 1", 0},
		{"insert into t(a,b) values(?,?)", 2},
		{"select 'a?b'", 0},
		{"select ? -- ?\n, ?", 2},
		{"/* ? */ ?", 1},
	}
	for _, c := range cases {
		if n := countPlaceholders(c.in); n != c.want {
			t.Errorf("countPlaceholders(%q) = %d, want %d", c.in, n, c.want)
		}
	}
}

func TestNormalizeSQL(t *testing.T) {
	cases := []struct {
		in    string
		want  string
		lower string
	}{
		{"/* hint */ select 1", "select 1", "select 1"},
		{"  SELECT 1;", "SELECT 1", "select 1"},
		{"-- c\nselect 1", "select 1", "select 1"},
		{"/* unclosed", "", ""},
		{"select /* mid */ 1", "select /* mid */ 1", "select /* mid */ 1"},
		{"   ", "", ""},
	}
	for _, c := range cases {
		got, lower := normalizeSQL(c.in)
		if got != c.want || lower != c.lower {
			t.Errorf("normalizeSQL(%q) = (%q, %q), want (%q, %q)", c.in, got, lower, c.want, c.lower)
		}
	}
}

func TestParseAutocommitValue(t *testing.T) {
	cases := []struct {
		in      string
		on      bool
		matched bool
	}{
		{"set autocommit=1", true, true},
		{"set autocommit=0", false, true},
		{"set autocommit = off", false, true},
		{"set @@session.autocommit=1", true, true},
		{"set session autocommit=on", true, true},
		{"set names utf8mb4", false, false},
		{"set sql_mode='x'", false, false},
	}
	for _, c := range cases {
		on, matched := parseAutocommitValue(c.in)
		if on != c.on || matched != c.matched {
			t.Errorf("parseAutocommitValue(%q) = (%v, %v), want (%v, %v)", c.in, on, matched, c.on, c.matched)
		}
	}
}

func TestExtractInsertTable(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"insert into sys_user(role, username) values(1, ?)", "sys_user"},
		{"insert ignore into `user` values(1)", "user"},
		{"INSERT INTO public.t(a) values(?)", "t"},
		{"insert into t set a=1", "t"},
		{"update t set a=1", ""},
	}
	for _, c := range cases {
		if got := extractInsertTable(c.in); got != c.want {
			t.Errorf("extractInsertTable(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizeSessionVarKey(t *testing.T) {
	cases := map[string]string{
		"@@session.transaction_isolation": "transaction_isolation",
		"@@GLOBAL.version":                "version",
		"@@version":                       "version",
		"@@character_set_client":          "character_set_client",
	}
	for in, want := range cases {
		if got := normalizeSessionVarKey(in); got != want {
			t.Errorf("normalizeSessionVarKey(%q) = %q, want %q", in, got, want)
		}
	}
}

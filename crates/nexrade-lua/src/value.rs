//! Conversion between Lua values and RESP values.

use mlua::{Lua, Value as LuaValue};
use nexrade_core::resp::{Resp, RespParser};

/// Convert a RESP value to a Lua value.
pub fn resp_to_lua(lua: &Lua, resp: Resp) -> mlua::Result<LuaValue<'_>> {
    match resp {
        Resp::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            Ok(LuaValue::Table(t))
        }
        Resp::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", e)?;
            Ok(LuaValue::Table(t))
        }
        Resp::Integer(n) => Ok(LuaValue::Integer(n)),
        Resp::BulkString(None) => Ok(LuaValue::Nil),
        Resp::BulkString(Some(b)) => {
            let s = lua.create_string(b.as_ref())?;
            Ok(LuaValue::String(s))
        }
        Resp::Array(None) => Ok(LuaValue::Nil),
        Resp::Array(Some(items)) => {
            let t = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                let v = resp_to_lua(lua, item)?;
                t.set(i + 1, v)?;
            }
            Ok(LuaValue::Table(t))
        }
        // RESP3 types — map to sensible Lua equivalents
        Resp::Null => Ok(LuaValue::Nil),
        Resp::Bool(b) => Ok(LuaValue::Boolean(b)),
        Resp::Double(f) => Ok(LuaValue::Number(f)),
        Resp::Map(pairs) => {
            let t = lua.create_table()?;
            for (k, v) in pairs {
                let lk = resp_to_lua(lua, k)?;
                let lv = resp_to_lua(lua, v)?;
                t.set(lk, lv)?;
            }
            Ok(LuaValue::Table(t))
        }
        Resp::Set(items) | Resp::Push(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                let v = resp_to_lua(lua, item)?;
                t.set(i + 1, v)?;
            }
            Ok(LuaValue::Table(t))
        }
        Resp::Raw(bytes) => {
            let mut parser = RespParser::new();
            parser.feed(&bytes);
            match parser.parse_one() {
                Ok(Some(parsed)) => resp_to_lua(lua, parsed),
                _ => Ok(LuaValue::Nil),
            }
        }
    }
}

/// Convert a Lua value to a RESP value.
pub fn lua_to_resp(val: LuaValue) -> Resp {
    match val {
        LuaValue::Nil => Resp::null(),
        LuaValue::Boolean(b) => Resp::int(b as i64),
        LuaValue::Integer(n) => Resp::int(n),
        LuaValue::Number(f) => Resp::int(f as i64),
        LuaValue::String(s) => Resp::bulk(bytes::Bytes::copy_from_slice(s.as_bytes())),
        LuaValue::Table(t) => {
            // Check for {ok = ...} or {err = ...}
            if let Ok(Some(ok_val)) = t.get::<_, Option<String>>("ok") {
                return Resp::SimpleString(ok_val);
            }
            if let Ok(Some(err_val)) = t.get::<_, Option<String>>("err") {
                return Resp::Error(err_val);
            }

            // Array table
            let mut items = Vec::new();
            let mut i = 1usize;
            loop {
                match t.get::<_, LuaValue>(i) {
                    Ok(LuaValue::Nil) => break,
                    Ok(v) => items.push(lua_to_resp(v)),
                    Err(_) => break,
                }
                i += 1;
            }
            Resp::array(items)
        }
        _ => Resp::null(),
    }
}

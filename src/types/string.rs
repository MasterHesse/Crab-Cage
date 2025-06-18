
use sled::Db;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum StringError {
    Sled(sled::Error),
    Utf8(FromUtf8Error),
    NotInteger,
    Overflow,
}

impl From<sled::Error> for StringError {
    fn from(e: sled::Error) -> Self {
        StringError::Sled(e)
    }
}

impl From<FromUtf8Error> for StringError {
    fn from(e: FromUtf8Error) -> Self {
        StringError::Utf8(e)
    }
}

/// SET key value -> "OK"
pub fn set(db: &Db, key: &str, val: &str) -> Result<String, StringError> {
    db.insert(key.as_bytes(), val.as_bytes())?;
    Ok("OK".into())
}

/// GET key -> Some(value) or None
pub fn get(db: &Db, key: &str) -> Result<Option<String>, StringError> {
    match db.get(key.as_bytes())? {
        Some(ivec) => {
            let v = String::from_utf8(ivec.to_vec())?;
            Ok(Some(v))
        }
        None => Ok(None),
    }
}

/// DEL key -> true(删除成功) / false(不存在)
pub fn del(db: &Db, key: &str) -> Result<bool, StringError> {
    Ok(db.remove(key.as_bytes())?.is_some())
}

/// INCR
pub fn incr(db: &Db, key: &str) -> Result<String, StringError> {
    loop {
        // 1. 读取旧值
        let old_opt: Option<String> =
            db.get(key)?
              .map(|ivec| String::from_utf8(ivec.to_vec()))
              .transpose()?;

        // 2. 解析或默认为 0
        let old_num = match &old_opt {
            Some(s) => s.parse::<i64>().map_err(|_| StringError::NotInteger)?,
            None    => 0,
        };

        // 3. 计算新值
        let new_num = old_num.checked_add(1).ok_or(StringError::Overflow)?;
        let new_bytes = new_num.to_string().into_bytes();

        // 4. 从 old_opt 构造一个期望值引用，不动用 old_opt 本身
        let expected: Option<&[u8]> = old_opt.as_ref().map(|s| s.as_bytes());

        // 5. CAS 操作
        let cas_res = db.compare_and_swap(key, expected, Some(new_bytes.clone()))?;
        if cas_res.is_ok() {
            // 成功就返回新值
            return Ok(new_num.to_string());
        }
        // 否则重试
    }
}

/// DECR
pub fn decr(db: &Db, key: &str) -> Result<String, StringError> {
    loop {
        let old_opt: Option<String> =
            db.get(key)?
              .map(|ivec| String::from_utf8(ivec.to_vec()))
              .transpose()?;

        let old_num = match &old_opt {
            Some(s) => s.parse::<i64>().map_err(|_| StringError::NotInteger)?,
            None    => 0,
        };

        let new_num = old_num.checked_sub(1).ok_or(StringError::Overflow)?;
        let new_bytes = new_num.to_string().into_bytes();

        let expected: Option<&[u8]> = old_opt.as_ref().map(|s| s.as_bytes());

        let cas_res = db.compare_and_swap(key, expected, Some(new_bytes.clone()))?;
        if cas_res.is_ok() {
            return Ok(new_num.to_string());
        }
    }
}

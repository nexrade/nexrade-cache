//! Geospatial command handlers — GEOADD, GEOPOS, GEODIST, GEORADIUS,
//! GEORADIUSBYMEMBER, GEOHASH, GEOSEARCH.

use crate::command::get_str;
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::{DataType, GeoData, GeoPoint};

// ── Earth radius & unit conversions ──────────────────────────────────────────

const EARTH_RADIUS_M: f64 = 6_372_797.560_856;

fn to_meters(value: f64, unit: &str) -> Result<f64> {
    match unit.to_uppercase().as_str() {
        "M" => Ok(value),
        "KM" => Ok(value * 1_000.0),
        "MI" => Ok(value * 1_609.344),
        "FT" => Ok(value * 0.3048),
        _ => Err(NexradeError::Generic(format!("unsupported unit: {}", unit))),
    }
}

fn from_meters(meters: f64, unit: &str) -> f64 {
    match unit.to_uppercase().as_str() {
        "KM" => meters / 1_000.0,
        "MI" => meters / 1_609.344,
        "FT" => meters / 0.3048,
        _ => meters, // default: m
    }
}

// ── Haversine distance (meters) ───────────────────────────────────────────────

pub fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1r = lat1.to_radians();
    let lat2r = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1r.cos() * lat2r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

// ── Geohash encoding (base32, 11 chars = 52-bit precision) ───────────────────

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Encode (longitude, latitude) to an 11-character geohash string.
pub fn geohash_encode(lon: f64, lat: f64) -> String {
    let mut min_lon = -180.0_f64;
    let mut max_lon = 180.0_f64;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;

    let mut result = Vec::with_capacity(11);
    let mut bits = 0u8;
    let mut bit_count = 0u8;
    let mut is_lon = true;

    for _ in 0..55 {
        let (mid, bit) = if is_lon {
            let m = (min_lon + max_lon) / 2.0;
            if lon >= m {
                max_lon = m;
                (m, 1u8)
            } else {
                min_lon = m;
                (m, 0u8)
            }
        } else {
            let m = (min_lat + max_lat) / 2.0;
            if lat >= m {
                max_lat = m;
                (m, 1u8)
            } else {
                min_lat = m;
                (m, 0u8)
            }
        };
        let _ = mid;
        is_lon = !is_lon;
        bits = (bits << 1) | bit;
        bit_count += 1;
        if bit_count == 5 {
            result.push(BASE32[bits as usize]);
            bits = 0;
            bit_count = 0;
            if result.len() == 11 {
                break;
            }
        }
    }

    String::from_utf8(result).unwrap_or_default()
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Get or create a GeoData value at `key` in `db_index`.
macro_rules! get_geo_mut {
    ($store_db:expr, $key:expr) => {{
        if $store_db.get(&$key[..]).is_none() {
            $store_db.insert($key.to_vec(), Entry::new(DataType::Geo(GeoData::new())));
        }
        match $store_db.get_mut(&$key[..]) {
            Some(e) => match &mut e.value {
                DataType::Geo(g) => g,
                _ => return Err(NexradeError::WrongType),
            },
            None => unreachable!(),
        }
    }};
}

fn get_geo_ro(entry: &Entry) -> Result<&GeoData> {
    match &entry.value {
        DataType::Geo(g) => Ok(g),
        _ => Err(NexradeError::WrongType),
    }
}

// ── GEOADD ────────────────────────────────────────────────────────────────────

/// `GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]`
pub async fn cmd_geoadd(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("geoadd".to_string()));
    }
    let key = get_str(args, 1, "GEOADD")?.as_bytes().to_vec();

    // Optional flags
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut idx = 2usize;
    while let Some(flag) = args
        .get(idx)
        .and_then(|a| a.as_str())
        .map(|s| s.to_uppercase())
    {
        match flag.as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "CH" => {
                ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    // Remaining args must be triples: lon lat member
    if (args.len() - idx) % 3 != 0 {
        return Err(NexradeError::SyntaxError);
    }

    let mut added = 0i64;
    let mut changed = 0i64;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let geo = get_geo_mut!(store_db, key);

    let mut i = idx;
    while i + 2 < args.len() {
        let lon: f64 = args[i]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(NexradeError::NotFloat)?;
        let lat: f64 = args[i + 1]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(NexradeError::NotFloat)?;
        if !(-180.0..=180.0).contains(&lon) || !(-85.051_129..=85.051_129).contains(&lat) {
            return Err(NexradeError::Generic(
                "ERR invalid longitude,latitude pair".to_string(),
            ));
        }
        let member = args[i + 2]
            .as_bytes()
            .map(|b| b.to_vec())
            .unwrap_or_else(|| args[i + 2].as_str().unwrap_or("").as_bytes().to_vec());

        let exists = geo.members.contains_key(&member);
        if xx && !exists {
            i += 3;
            continue;
        }
        if nx && exists {
            i += 3;
            continue;
        }
        if !exists {
            added += 1;
        } else {
            changed += 1;
        }
        geo.members.insert(
            member,
            GeoPoint {
                longitude: lon,
                latitude: lat,
            },
        );
        i += 3;
    }

    Ok(Resp::Integer(if ch { added + changed } else { added }))
}

// ── GEOPOS ────────────────────────────────────────────────────────────────────

/// `GEOPOS key member [member ...]`
pub async fn cmd_geopos(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("geopos".to_string()));
    }
    let key = get_str(args, 1, "GEOPOS")?.as_bytes().to_vec();
    let store_db = db.store.db(db_index).read_for(&key);
    let geo_opt = store_db.get_ro(&key);

    let mut results = Vec::new();
    for arg in args.get(2..).unwrap_or(&[]) {
        let member = arg
            .as_bytes()
            .map(|b| b.to_vec())
            .unwrap_or_else(|| arg.as_str().unwrap_or("").as_bytes().to_vec());

        let pos = geo_opt.and_then(|e| match &e.value {
            DataType::Geo(g) => g.members.get(&member),
            _ => None,
        });

        match pos {
            Some(pt) => {
                results.push(Resp::array(vec![
                    Resp::bulk_str(format!("{:.17}", pt.longitude)),
                    Resp::bulk_str(format!("{:.17}", pt.latitude)),
                ]));
            }
            None => {
                results.push(Resp::Array(None));
            }
        }
    }
    Ok(Resp::array(results))
}

// ── GEODIST ───────────────────────────────────────────────────────────────────

/// `GEODIST key member1 member2 [m|km|mi|ft]`
pub async fn cmd_geodist(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("geodist".to_string()));
    }
    let key = get_str(args, 1, "GEODIST")?.as_bytes().to_vec();
    let m1 = args[2]
        .as_bytes()
        .map(|b| b.to_vec())
        .unwrap_or_else(|| args[2].as_str().unwrap_or("").as_bytes().to_vec());
    let m2 = args[3]
        .as_bytes()
        .map(|b| b.to_vec())
        .unwrap_or_else(|| args[3].as_str().unwrap_or("").as_bytes().to_vec());
    let unit = args.get(4).and_then(|a| a.as_str()).unwrap_or("m");

    let store_db = db.store.db(db_index).read_for(&key);
    let entry = match store_db.get_ro(&key) {
        Some(e) => e,
        None => return Ok(Resp::Array(None)),
    };
    let geo = get_geo_ro(entry)?;

    let p1 = match geo.members.get(&m1) {
        Some(p) => p,
        None => return Ok(Resp::Array(None)),
    };
    let p2 = match geo.members.get(&m2) {
        Some(p) => p,
        None => return Ok(Resp::Array(None)),
    };

    let dist_m = haversine_m(p1.longitude, p1.latitude, p2.longitude, p2.latitude);
    let dist = from_meters(dist_m, unit);
    Ok(Resp::bulk_str(format!("{:.4}", dist)))
}

// ── GEOHASH ───────────────────────────────────────────────────────────────────

/// `GEOHASH key member [member ...]`
pub async fn cmd_geohash(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("geohash".to_string()));
    }
    let key = get_str(args, 1, "GEOHASH")?.as_bytes().to_vec();
    let store_db = db.store.db(db_index).read_for(&key);
    let geo_opt = store_db.get_ro(&key);

    let mut results = Vec::new();
    for arg in args.get(2..).unwrap_or(&[]) {
        let member = arg
            .as_bytes()
            .map(|b| b.to_vec())
            .unwrap_or_else(|| arg.as_str().unwrap_or("").as_bytes().to_vec());

        let hash = geo_opt
            .and_then(|e| match &e.value {
                DataType::Geo(g) => g.members.get(&member),
                _ => None,
            })
            .map(|pt| geohash_encode(pt.longitude, pt.latitude));

        match hash {
            Some(h) => results.push(Resp::bulk_str(h)),
            None => results.push(Resp::Array(None)),
        }
    }
    Ok(Resp::array(results))
}

// ── Shared search logic ───────────────────────────────────────────────────────

struct GeoSearchOpts<'a> {
    center_lon: f64,
    center_lat: f64,
    radius_m: f64,
    asc: bool,
    desc: bool,
    count: Option<usize>,
    withcoord: bool,
    withdist: bool,
    unit: &'a str,
}

fn geo_search_results(geo: &GeoData, opts: &GeoSearchOpts<'_>) -> Vec<Resp> {
    let mut hits: Vec<(Vec<u8>, f64, f64, f64)> = geo
        .members
        .iter()
        .filter_map(|(member, pt)| {
            let dist = haversine_m(opts.center_lon, opts.center_lat, pt.longitude, pt.latitude);
            if dist <= opts.radius_m {
                Some((member.clone(), dist, pt.longitude, pt.latitude))
            } else {
                None
            }
        })
        .collect();

    if opts.asc {
        hits.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    } else if opts.desc {
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    if let Some(n) = opts.count {
        hits.truncate(n);
    }

    hits.into_iter()
        .map(|(member, dist, lon, lat)| {
            if opts.withdist || opts.withcoord {
                let mut row = vec![Resp::bulk(bytes::Bytes::from(member))];
                if opts.withdist {
                    row.push(Resp::bulk_str(format!(
                        "{:.4}",
                        from_meters(dist, opts.unit)
                    )));
                }
                if opts.withcoord {
                    row.push(Resp::array(vec![
                        Resp::bulk_str(format!("{:.17}", lon)),
                        Resp::bulk_str(format!("{:.17}", lat)),
                    ]));
                }
                Resp::array(row)
            } else {
                Resp::bulk(bytes::Bytes::from(member))
            }
        })
        .collect()
}

// ── GEORADIUS ─────────────────────────────────────────────────────────────────

/// `GEORADIUS key lon lat radius m|km|mi|ft [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]`
pub async fn cmd_georadius(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 6 {
        return Err(NexradeError::WrongArity("georadius".to_string()));
    }
    let key = get_str(args, 1, "GEORADIUS")?.as_bytes().to_vec();
    let lon: f64 = args[2]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(NexradeError::NotFloat)?;
    let lat: f64 = args[3]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(NexradeError::NotFloat)?;
    let radius_val: f64 = args[4]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(NexradeError::NotFloat)?;
    let unit = args[5].as_str().ok_or(NexradeError::SyntaxError)?;
    let radius_m = to_meters(radius_val, unit)?;

    let mut withcoord = false;
    let mut withdist = false;
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    let mut i = 6;
    while i < args.len() {
        match args[i].as_str().unwrap_or("").to_uppercase().as_str() {
            "WITHCOORD" => {
                withcoord = true;
            }
            "WITHDIST" => {
                withdist = true;
            }
            "ASC" => {
                asc = true;
            }
            "DESC" => {
                desc = true;
            }
            "COUNT" => {
                i += 1;
                count = args[i].as_str().and_then(|s| s.parse().ok());
            }
            _ => {}
        }
        i += 1;
    }

    let store_db = db.store.db(db_index).read_for(&key);
    let entry = match store_db.get_ro(&key) {
        Some(e) => e,
        None => return Ok(Resp::array(vec![])),
    };
    let geo = get_geo_ro(entry)?;
    let opts = GeoSearchOpts {
        center_lon: lon,
        center_lat: lat,
        radius_m,
        asc,
        desc,
        count,
        withcoord,
        withdist,
        unit,
    };
    Ok(Resp::array(geo_search_results(geo, &opts)))
}

// ── GEORADIUSBYMEMBER ─────────────────────────────────────────────────────────

/// `GEORADIUSBYMEMBER key member radius m|km|mi|ft [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]`
pub async fn cmd_georadiusbymember(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("georadiusbymember".to_string()));
    }
    let key = get_str(args, 1, "GEORADIUSBYMEMBER")?.as_bytes().to_vec();
    let member = args[2]
        .as_bytes()
        .map(|b| b.to_vec())
        .unwrap_or_else(|| args[2].as_str().unwrap_or("").as_bytes().to_vec());
    let radius_val: f64 = args[3]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(NexradeError::NotFloat)?;
    let unit = args[4].as_str().ok_or(NexradeError::SyntaxError)?;
    let radius_m = to_meters(radius_val, unit)?;

    let mut withcoord = false;
    let mut withdist = false;
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    let mut i = 5;
    while i < args.len() {
        match args[i].as_str().unwrap_or("").to_uppercase().as_str() {
            "WITHCOORD" => {
                withcoord = true;
            }
            "WITHDIST" => {
                withdist = true;
            }
            "ASC" => {
                asc = true;
            }
            "DESC" => {
                desc = true;
            }
            "COUNT" => {
                i += 1;
                count = args[i].as_str().and_then(|s| s.parse().ok());
            }
            _ => {}
        }
        i += 1;
    }

    let store_db = db.store.db(db_index).read_for(&key);
    let entry = match store_db.get_ro(&key) {
        Some(e) => e,
        None => return Ok(Resp::array(vec![])),
    };
    let geo = get_geo_ro(entry)?;

    let center = geo
        .members
        .get(&member)
        .ok_or_else(|| NexradeError::Generic("ERR could not hget key".to_string()))?;
    let (clon, clat) = (center.longitude, center.latitude);
    let opts = GeoSearchOpts {
        center_lon: clon,
        center_lat: clat,
        radius_m,
        asc,
        desc,
        count,
        withcoord,
        withdist,
        unit,
    };
    Ok(Resp::array(geo_search_results(geo, &opts)))
}

// ── GEOSEARCH ─────────────────────────────────────────────────────────────────

/// `GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat BYRADIUS radius unit | BYBOX w h unit [ASC|DESC] [COUNT n] [WITHCOORD] [WITHDIST]`
pub async fn cmd_geosearch(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 6 {
        return Err(NexradeError::WrongArity("geosearch".to_string()));
    }
    let key = get_str(args, 1, "GEOSEARCH")?.as_bytes().to_vec();

    // Parse center
    let (center_lon, center_lat, mut idx) = match args
        .get(2)
        .and_then(|a| a.as_str())
        .map(|s| s.to_uppercase())
        .as_deref()
    {
        Some("FROMLONLAT") => {
            let lon: f64 = args
                .get(3)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or(NexradeError::NotFloat)?;
            let lat: f64 = args
                .get(4)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or(NexradeError::NotFloat)?;
            (lon, lat, 5usize)
        }
        Some("FROMMEMBER") => {
            // Need to look up the member first
            let member = args[3]
                .as_bytes()
                .map(|b| b.to_vec())
                .unwrap_or_else(|| args[3].as_str().unwrap_or("").as_bytes().to_vec());
            let store_db = db.store.db(db_index).read_for(&key);
            let entry = store_db
                .get_ro(&key)
                .ok_or_else(|| NexradeError::Generic("ERR no such key".to_string()))?;
            let geo = get_geo_ro(entry)?;
            let pt = geo
                .members
                .get(&member)
                .ok_or_else(|| NexradeError::Generic("ERR could not hget key".to_string()))?;
            (pt.longitude, pt.latitude, 4usize)
        }
        _ => return Err(NexradeError::SyntaxError),
    };

    // Parse BYRADIUS or BYBOX
    let (radius_m, unit_str) = match args
        .get(idx)
        .and_then(|a| a.as_str())
        .map(|s| s.to_uppercase())
        .as_deref()
    {
        Some("BYRADIUS") => {
            let r: f64 = args
                .get(idx + 1)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or(NexradeError::NotFloat)?;
            let u = args
                .get(idx + 2)
                .and_then(|a| a.as_str())
                .ok_or(NexradeError::SyntaxError)?;
            let rm = to_meters(r, u)?;
            idx += 3;
            (rm, u.to_string())
        }
        Some("BYBOX") => {
            // Use half-diagonal as radius approximation
            let w: f64 = args
                .get(idx + 1)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or(NexradeError::NotFloat)?;
            let h: f64 = args
                .get(idx + 2)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or(NexradeError::NotFloat)?;
            let u = args
                .get(idx + 3)
                .and_then(|a| a.as_str())
                .ok_or(NexradeError::SyntaxError)?;
            let wm = to_meters(w, u)?;
            let hm = to_meters(h, u)?;
            let rm = ((wm / 2.0).powi(2) + (hm / 2.0).powi(2)).sqrt();
            idx += 4;
            (rm, u.to_string())
        }
        _ => return Err(NexradeError::SyntaxError),
    };

    let mut withcoord = false;
    let mut withdist = false;
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    while idx < args.len() {
        match args[idx].as_str().unwrap_or("").to_uppercase().as_str() {
            "WITHCOORD" => {
                withcoord = true;
            }
            "WITHDIST" => {
                withdist = true;
            }
            "ASC" => {
                asc = true;
            }
            "DESC" => {
                desc = true;
            }
            "COUNT" => {
                idx += 1;
                count = args
                    .get(idx)
                    .and_then(|a| a.as_str())
                    .and_then(|s| s.parse().ok());
            }
            _ => {}
        }
        idx += 1;
    }

    let store_db = db.store.db(db_index).read_for(&key);
    let entry = match store_db.get_ro(&key) {
        Some(e) => e,
        None => return Ok(Resp::array(vec![])),
    };
    let geo = get_geo_ro(entry)?;
    let opts = GeoSearchOpts {
        center_lon,
        center_lat,
        radius_m,
        asc,
        desc,
        count,
        withcoord,
        withdist,
        unit: unit_str.as_str(),
    };
    Ok(Resp::array(geo_search_results(geo, &opts)))
}

[![Build Status](https://travis-ci.org/shove70/database.svg?branch=master)](https://travis-ci.org/shove70/database)
[![GitHub tag](https://img.shields.io/github/tag/shove70/database.svg?maxAge=86400)](https://github.com/shove70/database/releases)
[![Dub downloads](https://img.shields.io/dub/dt/database.svg)](http://code.dlang.org/packages/database)

# database
A lightweight native MySQL/MariaDB & PostgreSQL driver written in D.

The goal is a native driver that re-uses the same buffers and the stack as much as possible,
avoiding unnecessary allocations and work for the garbage collector

Native. No link, No harm :)

## MySQL example
```d
import std.datetime;
import std.stdio;
import mysql;

void main() {
	auto conn = new Connection("127.0.0.1", "root", "pwd", "test", 3306);

	// change database
	conn.use("mewmew");

	// simple insert statement
	conn.query("insert into users (name, email) values (?, ?)", "frank", "thetank@cowabanga.com");
	auto id = conn.lastInsertId;

	struct User {
		string name;
		string email;
	}

	// simple select statement
	User[] users;
	conn.query("select name, email from users where id > ?", 13, (MySQLRow row) {
		users ~= row.get!User;
	});

	// simple select statement
	conn.query("select name, email from users where id > ?", 13, (MySQLRow row) {
		writeln(row["name"], row["email"]);
	});

	// batch inserter - inserts in packets of 128k bytes
	auto insert = inserter(conn, "users_copy", "name", "email");
	foreach(user; users)
		insert.row(user.name, user.email);
	insert.flush;

	// re-usable prepared statements
	auto upd = conn.prepare("update users set sequence = ?, login_at = ?, secret = ? where id = ?");
	ubyte[] bytes = [0x4D, 0x49, 0x4C, 0x4B];
	foreach(i; 0..100)
		conn.exec(upd, i, Clock.currTime, MySQLBinary(bytes), i);

	// passing variable or large number of arguments
	string[] names;
	string[] emails;
	int[] ids = [1, 1, 3, 5, 8, 13];
	conn.query("select name from users where id in " ~ ids.placeholders, ids, (MySQLRow row) {
		writeln(row.name.peek!(char[])); // peek() avoids allocation - cannot use result outside delegate
		names ~= row.name.get!string;    // get() duplicates - safe to use result outside delegate
		emails ~= row.email.get!string;
	});

	// another query example
	conn.query("select id, name, email from users where id > ?", 13, (size_t index /*optional*/, MySQLHeader header /*optional*/, MySQLRow row) {
		writeln(header[0].name, ": ", row.id.get!int);
		return (index < 5); // optionally return false to discard remaining results
	});

	// structured row
	conn.query("select name, email from users where length(name) > ?", 5, (MySQLRow row) {
		auto user = row.get!User; // default is strict.yesIgnoreNull - a missing field in the row will throw
		// auto user = row.get!(User, Strict.yes); // missing or null will throw
		// auto user = row.get!(User, Strict.no);  // missing or null will just be ignored
		writeln(user);
	});

	// structured row with nested structs
	struct GeoRef {
		double lat;
		double lng;
	}

	struct Place {
		string name;
		GeoRef location;
	}

	conn.query("select name, lat as `location.lat`, lng as `location.lng` from places", (MySQLRow row) {
		auto place = row.get!Place;
		writeln(place.location);
	});

	// structured row annotations
	struct PlaceFull {
		uint id;
		string name;
		@optional string thumbnail;    // ok to be null or missing
		@optional GeoRef location;     // nested fields ok to be null or missing
		@optional @as("contact_person") string contact; // optional, and sourced from field contact_person instead

		@ignore File tumbnail;    // completely ignored
	}

	conn.query("select id, name, thumbnail, lat as `location.lat`, lng as `location.lng`, contact_person from places", (MySQLRow row) {
		auto place = row.get!PlaceFull;
		writeln(place.location);
	});


	// automated struct member uncamelcase
	struct PlaceOwner {
	@snakeCase:
		uint placeID;            // matches placeID and place_id
		uint locationId;         // matches locationId and location_id
		string ownerFirstName;   // matches ownerFirstName and owner_first_name
		string ownerLastName;    // matches ownerLastName and owner_last_name
		string feedURL;          // matches feedURL and feed_url
	}

	conn.close();
}
```


## PGSQL example
```d
import std.stdio;
import postgresql;

@snakeCase struct PlaceOwner {
@snakeCase:
    @sqlkey() uint placeID; // matches place_id
    uint locationId; // matches location_id
    string ownerName; // matches owner_name
    string feedURL; // matches feed_url
}

PgSQLDB db;
db.connect("127.0.0.1", 5432, "postgres", "postgres", "postgres");
db.exec(`CREATE TABLE IF NOT EXISTS company(
ID INT PRIMARY KEY	NOT NULL,
name		TEXT	NOT NULL,
age			INT		NOT NULL,
address		CHAR(50),
salary		REAL,
join_date	DATE
);`);
assert(db.hasTable("company"));
db.create!PlaceOwner;
db.insert(PlaceOwner(1, 1, "foo", ""));
db.insert(PlaceOwner(2, 1, "bar", ""));
db.insert(PlaceOwner(3, 3, "baz", ""));
auto s = db.selectOneWhere!(PlaceOwner, "owner_name=?")("bar");
assert(s.placeID == 2);
foreach (row; db.selectAllWhere!(PlaceOwner, "location_id=?")(1))
    writeln(row);
assert(db.exec("drop table place_owner"));
assert(db.exec("drop table company"));
db.close();
```

## SQLite example
```d
import database.sqlite.db;

auto db = new SQLite3DB("file.db");
db.exec("INSERT INTO user (name, id) VALUES (?, ?)", name, id);
```

```d
struct User {
    ulong id;
    string name;
    void[] pixels;
};

User[] users;
auto q = db.query("SELECT id,name FROM user");
while(q.step()) {
    users ~= q.get!User;
}
```
# Overview

Simple SQL CLI, building upon `pg` and `mysql` CLI's with a few additional features

![Screenshot Example](https://raw.githubusercontent.com/azvaliev/sql/master/assets/main.png)

- One CLI for Postgres and MySQL
- Easy multiline editing of queries
- Unified command to view table schema. `DESCRIBE X`
- Display results in a scrollable table, no wrapping


## Installation

```bash
go install github.com/azvaliev/sql@latest
```

## Usage

Specify what kind of database you're connecting to via `-mysql` or `-psql` flags (required).
For additional arguments see `sql --help`

```bash
sql (-mysql OR -psql) (... additional options) --database=example
```

### Application Usage

In the query text area, type any SQL statement followed by `;` and hit enter to send the query. Results will be displayed in the above space on the screen.

The text area is multi-line and you can use either the mouse or arrow keys to navigate through the text area.

#### Unified DESCRIBE command

I've ported the MySQL `DESCRIBE X` command for easily viewing details about a table. See [MySQL documentation](https://dev.mysql.com/doc/refman/8.4/en/show-columns.html) for details

#### Handling overflowing results

When editing the text area, one can scroll the results section using `ctrl` or `option` (MacOS) + corresponding arrow for direction to scroll.

Example: To scroll results up, (`ctrl` or `option`) + `↑`

#### Safe Mode (MySQL)

MySQL has an option called safe mode, you can enable this feature using the `-safe` flag when running this application. It helps prevent unbounded update/delete operations

See [MySQL Documentation](https://dev.mysql.com/doc/refman/8.4/en/mysql-tips.html#safe-updates) for more details

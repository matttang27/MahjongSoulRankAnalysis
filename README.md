# Mahjong Soul Rank Analysis

How does Mahjong Soul rank correlate to score & placement?

Fetches data from Amae Koromo API:

Ex: Taking games between 1659809600000 and 1759828200000 in Jade South games:

https://5-data.amae-koromo.com/api/v2/pl4/games/1759828200000/1659809600000?limit=100000&descending=true&mode=12

returns a list of games in that time range, limit unknown

```
[
	{
		"_id": "90LXpfcSY9g",
		"modeId": 9,
		"uuid": "90LXpfcSY9g",
		"startTime": 1759826807,
		"endTime": 1759827889,
		"players": [
			{
				"accountId": 19643057,
				"nickname": "OohLaLa",
				"level": 10303,
				"score": 44200,
				"gradingScore": 115
			},
			{
				"accountId": 68151770,
				"nickname": "つちご",
				"level": 10303,
				"score": 38900,
				"gradingScore": 59
			},
			{
				"accountId": 11135218,
				"nickname": "那就一起摆吧",
				"level": 10302,
				"score": 18600,
				"gradingScore": -11
			},
			{
				"accountId": 71501780,
				"nickname": "Toprunj",
				"level": 10303,
				"score": -1700,
				"gradingScore": -161
			}
		],
		"_masked": true
	}, 
    ...
]
```

To fetch games:
Run fetch to API, check furthest timestamp, fetch again with that timestamp as the new end time.

Stored in SQLite database with simplified schema:

```sql
CREATE TABLE games (
    id TEXT PRIMARY KEY,
    mode INTEGER,
    endTime INTEGER,
    player1_level INTEGER,
    player1_score INTEGER,
    player1_gradingScore INTEGER,
    player2_level INTEGER,
    player2_score INTEGER,
    player2_gradingScore INTEGER,
    player3_level INTEGER,
    player3_score INTEGER,
    player3_gradingScore INTEGER,
    player4_level INTEGER,
    player4_score INTEGER,
    player4_gradingScore INTEGER
);
```

Modes:
Gold South: 9
Jade South: 12
Throne South: 16

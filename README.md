# fantasy-league
Streamlit app for fantasy league

This was a simple app built to use by friends for [League of Legends Worlds 24 event](https://lol.fandom.com/wiki/2024_Season_World_Championship)
as a fantasy team builder.

The main purpose was to have fun and so it was built quickly and with shortcuts taken, so it wouldn't work 
in any serious environment or capacity. For example - the app contains users(people playing) and they each have 
their own account, however no serious measures were taken to protect these accounts (apart from hashing a simple password).

Key parts of the project:
- Admin panel:
  - Admin users are able to edit other users information
  - event structure
  - fantasy player information
  - fantasy player pricing - each fantasy player has a cost associated with him, which can be calculated 
    based on previous matches performance. Each player that is building a fantasy team has limited amount
    of points to spend on his team.
  - edit fantasy teams
  - upload match data
- Login panel:
  - used to authenticate user
- Main panel:
  - Each user is able to create their fantasy team for specific stages
  - view other competitors teams
  - fantasy team results that are ranked
  - view each fantasy player results from previous stages, matches

**Data used is aggregated by [Orackle's Elixir](https://oracleselixir.com/about)**
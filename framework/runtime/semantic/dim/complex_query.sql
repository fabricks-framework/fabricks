/*
|0|Not Royal Stock|
|1|Royal Stock|
|2|Rarely Visited|
|3|Forgotten in Tower|
|4|Royal Clearance|
*/
select *
from
values
    /*
    |0|Not Royal Stock|
    |1|Royal Stock|
    |2|Rarely Visited|
    |3|Forgotten in Tower|
    |4|Royal Clearance|
  */
    -- Prince 02 (The Charming Prince)
    ('02', 0, 2),  -- Not in royal stock -> 2 - available at court
    ('02', 1, 3),  -- In royal stock -> 3 - available / 1st priority at royal ball
    ('02', 2, 2),  -- Rarely visited -> 2 - available at court
    ('02', 3, 2),  -- Forgotten in tower -> 2 - available at court
    ('02', 4, 1),  -- Royal clearance -> 1 - available (lowest priority, no dancing allowed)
    ('02', null, 1),
    -- Princess 03 (The Graceful Princess)
    ('03', 0, 2),  -- Not in royal stock -> 2 - available at court
    ('03', 1, 3),  -- In royal stock -> 3 - available
    ('03', 2, 2),  -- Rarely visited -> 2 - available at court
    ('03', 3, 2),  -- Forgotten in tower -> 2 - available at court
    ('03', 4, 1),  -- Royal clearance -> 1 - available (lowest priority, no dancing allowed)
    ('03', null, 1),
    -- Prince 04 (The Banished Prince)
    ('04', 0, 0),  -- n/a -> 0 - banished from kingdom
    ('04', 1, 0),  -- n/a -> 0 - banished from kingdom
    ('04', 2, 0),  -- n/a -> 0 - banished from kingdom
    ('04', 3, 0),  -- n/a -> 0 - banished from kingdom
    ('04', 4, 0),  -- n/a -> 0 - banished from kingdom
    ('04', null, 0),
    -- Princess 05 (The Sleeping Princess)
    ('05', 0, 0),  -- n/a -> 0 - fast asleep
    ('05', 1, 0),  -- n/a -> 0 - fast asleep
    ('05', 2, 0),  -- n/a -> 0 - fast asleep
    ('05', 3, 0),  -- n/a -> 0 - fast asleep
    ('05', 4, 0),  -- n/a -> 0 - fast asleep
    ('05', null, 0),
    -- Prince 06 (The Retired Prince)
    ('06', 0, 0),  -- Not available -> 0 - retired to countryside
    ('06', 1, 0),  -- Not available -> 0 - retired to countryside
    ('06', 2, 0),  -- Not available -> 0 - retired to countryside
    ('06', 3, 0),  -- Not available -> 0 - retired to countryside
    ('06', 4, 1),  -- Royal clearance (Replaced by younger prince) -> 1 - available (lowest priority, no dancing allowed)
    ('06', null, 0),
    -- Princess 07 (The Former Court Princess)
    ('07', 0, 0),  -- Not available -> 0 - left the court
    ('07', 1, 0),  -- Not available -> 0 - left the court
    ('07', 2, 0),  -- Not available -> 0 - left the court
    ('07', 3, 0),  -- Not available -> 0 - left the court
    ('07', 4, 1),  -- Royal clearance (Duties discontinued) -> 1 - available (lowest priority, no dancing allowed)
    ('07', null, 0),
    -- Prince 08 (The Liquidating Prince)
    ('08', 0, 0),  -- Not available -> 0 - selling the castle
    ('08', 1, 0),  -- Not available -> 0 - selling the castle
    ('08', 2, 0),  -- Not available -> 0 - selling the castle
    ('08', 3, 0),  -- Not available -> 0 - selling the castle
    ('08', 4, 1),  -- Royal clearance (Estate sale) -> 1 - available (lowest priority, no dancing allowed)
    ('08', null, 0),
    -- Princess 09-14 (The Lost Princesses)
    ('09', 0, 0),  -- n/a -> 0 - lost in the enchanted forest
    ('09', 1, 0),
    ('09', 2, 0),
    ('09', 3, 0),
    ('09', 4, 0),
    ('09', null, 0),
    ('10', 0, 0),
    ('10', 1, 0),
    ('10', 2, 0),
    ('10', 3, 0),
    ('10', 4, 0),
    ('10', null, 0),
    ('11', 0, 0),
    ('11', 1, 0),
    ('11', 2, 0),
    ('11', 3, 0),
    ('11', 4, 0),
    ('11', null, 0),
    ('12', 0, 0),
    ('12', 1, 0),
    ('12', 2, 0),
    ('12', 3, 0),
    ('12', 4, 0),
    ('12', null, 0),
    ('13', 0, 0),
    ('13', 1, 0),
    ('13', 2, 0),
    ('13', 3, 0),
    ('13', 4, 0),
    ('13', null, 0),
    ('14', 0, 0),
    ('14', 1, 0),
    ('14', 2, 0),
    ('14', 3, 0),
    ('14', 4, 0),
    ('14', null, 0),
    -- Prince 15 (The Service Prince)
    ('15', 0, 0),  -- Not available -> 0 - in royal service elsewhere
    ('15', 1, 0),  -- Not available -> 0 - in royal service elsewhere
    ('15', 2, 0),  -- Not available -> 0 - in royal service elsewhere
    ('15', 3, 0),  -- Not available -> 0 - in royal service elsewhere
    ('15', 4, 1),  -- Royal clearance (Special service) -> 1 - available (lowest priority, no dancing allowed)
    ('15', null, 0),
    -- Princes 20, 30, 40 (The Distant Princes)
    ('20', 0, 0),
    ('20', 1, 0),
    ('20', 2, 0),
    ('20', 3, 0),
    ('20', 4, 0),
    ('20', null, 0),
    ('30', 0, 0),
    ('30', 1, 0),
    ('30', 2, 0),
    ('30', 3, 0),
    ('30', 4, 0),
    ('30', null, 0),
    ('40', 0, 0),
    ('40', 1, 0),
    ('40', 2, 0),
    ('40', 3, 0),
    ('40', 4, 0),
    ('40', null, 0) as royal_court_status(royal_id, crown_jewels_status_id, royal_availability)

-- ============================================================
-- Property Management App — Seed Data
-- Alex's three rental properties, ~3 years of income & expenses
-- ============================================================

-- PROPERTIES
-- monthly_rent reflects current (2026) rate
INSERT INTO properties (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
VALUES
  (1, 'Lafayette Condo',        '412 Creekside Dr, Apt 3B', 'Lafayette',     'IN', '47901', 'Condo',         'Jordan Mills',  1300.00),
  (2, 'West Lafayette House',   '819 Maple Ridge Rd',        'West Lafayette','IN', '47906', 'Single-Family', 'Priya Nair',    1825.00),
  (3, 'Champaign Townhome',     '204 Elmwood Ct, Unit A',    'Champaign',     'IL', '61820', 'Townhome',      'Derek Solis',   1450.00);


-- ============================================================
-- INCOME
-- Annual rent rates:
--   Property 1: $1,150 (2023) | $1,225 (2024) | $1,300 (2025-2026)
--   Property 2: $1,650 (2023) | $1,725 (2024) | $1,825 (2025-2026)
--   Property 3: $1,300 (2023) | $1,375 (2024) | $1,450 (2025-2026)
--
-- Coverage: January 2023 – March 2026
-- Missed payments: P1 Aug 2023, P1 Feb 2025
--                  P2 May 2024, P2 Oct 2025
--                  P3 Mar 2024, P3 Jul 2025
-- ============================================================

INSERT INTO income (income_id, property_id, amount, date, description) VALUES

-- Property 1 — Lafayette Condo
-- 2023: $1,150/mo (missed: Aug 2023)
(1,  1, 1150.00, '2023-01-05', 'January 2023 rent'),
(2,  1, 1150.00, '2023-02-03', 'February 2023 rent'),
(3,  1, 1150.00, '2023-03-04', 'March 2023 rent'),
(4,  1, 1150.00, '2023-04-04', 'April 2023 rent'),
(5,  1, 1150.00, '2023-05-03', 'May 2023 rent'),
(6,  1, 1150.00, '2023-06-03', 'June 2023 rent'),
(7,  1, 1150.00, '2023-07-05', 'July 2023 rent'),
-- August 2023 missed
(8,  1, 2300.00, '2023-09-06', 'September 2023 rent + August back payment'),
(9,  1, 1150.00, '2023-10-04', 'October 2023 rent'),
(10, 1, 1150.00, '2023-11-03', 'November 2023 rent'),
(11, 1, 1150.00, '2023-12-05', 'December 2023 rent'),
-- 2024: $1,225/mo
(12, 1, 1225.00, '2024-01-04', 'January 2024 rent'),
(13, 1, 1225.00, '2024-02-03', 'February 2024 rent'),
(14, 1, 1225.00, '2024-03-03', 'March 2024 rent'),
(15, 1, 1225.00, '2024-04-04', 'April 2024 rent'),
(16, 1, 1225.00, '2024-05-03', 'May 2024 rent'),
(17, 1, 1225.00, '2024-06-05', 'June 2024 rent'),
(18, 1, 1225.00, '2024-07-05', 'July 2024 rent'),
(19, 1, 1225.00, '2024-08-03', 'August 2024 rent'),
(20, 1, 1225.00, '2024-09-05', 'September 2024 rent'),
(21, 1, 1225.00, '2024-10-04', 'October 2024 rent'),
(22, 1, 1225.00, '2024-11-04', 'November 2024 rent'),
(23, 1, 1225.00, '2024-12-04', 'December 2024 rent'),
-- 2025: $1,300/mo (missed: Feb 2025)
(24, 1, 1300.00, '2025-01-04', 'January 2025 rent'),
-- February 2025 missed
(25, 1, 2600.00, '2025-03-05', 'March 2025 rent + February back payment'),
(26, 1, 1300.00, '2025-04-03', 'April 2025 rent'),
(27, 1, 1300.00, '2025-05-03', 'May 2025 rent'),
(28, 1, 1300.00, '2025-06-04', 'June 2025 rent'),
(29, 1, 1300.00, '2025-07-03', 'July 2025 rent'),
(30, 1, 1300.00, '2025-08-05', 'August 2025 rent'),
(31, 1, 1300.00, '2025-09-04', 'September 2025 rent'),
(32, 1, 1300.00, '2025-10-03', 'October 2025 rent'),
(33, 1, 1300.00, '2025-11-05', 'November 2025 rent'),
(34, 1, 1300.00, '2025-12-04', 'December 2025 rent'),
-- 2026: $1,300/mo (no increase)
(35, 1, 1300.00, '2026-01-03', 'January 2026 rent'),
(36, 1, 1300.00, '2026-02-04', 'February 2026 rent'),
(37, 1, 1300.00, '2026-03-04', 'March 2026 rent'),

-- Property 2 — West Lafayette House
-- 2023: $1,650/mo
(38, 2, 1650.00, '2023-01-03', 'January 2023 rent'),
(39, 2, 1650.00, '2023-02-03', 'February 2023 rent'),
(40, 2, 1650.00, '2023-03-03', 'March 2023 rent'),
(41, 2, 1650.00, '2023-04-04', 'April 2023 rent'),
(42, 2, 1650.00, '2023-05-04', 'May 2023 rent'),
(43, 2, 1650.00, '2023-06-03', 'June 2023 rent'),
(44, 2, 1650.00, '2023-07-05', 'July 2023 rent'),
(45, 2, 1650.00, '2023-08-03', 'August 2023 rent'),
(46, 2, 1650.00, '2023-09-05', 'September 2023 rent'),
(47, 2, 1650.00, '2023-10-04', 'October 2023 rent'),
(48, 2, 1650.00, '2023-11-03', 'November 2023 rent'),
(49, 2, 1650.00, '2023-12-05', 'December 2023 rent'),
-- 2024: $1,725/mo (missed: May 2024)
(50, 2, 1725.00, '2024-01-04', 'January 2024 rent'),
(51, 2, 1725.00, '2024-02-03', 'February 2024 rent'),
(52, 2, 1725.00, '2024-03-03', 'March 2024 rent'),
(53, 2, 1725.00, '2024-04-04', 'April 2024 rent'),
-- May 2024 missed
(54, 2, 3450.00, '2024-06-05', 'June 2024 rent + May back payment'),
(55, 2, 1725.00, '2024-07-05', 'July 2024 rent'),
(56, 2, 1725.00, '2024-08-03', 'August 2024 rent'),
(57, 2, 1725.00, '2024-09-05', 'September 2024 rent'),
(58, 2, 1725.00, '2024-10-04', 'October 2024 rent'),
(59, 2, 1725.00, '2024-11-04', 'November 2024 rent'),
(60, 2, 1725.00, '2024-12-04', 'December 2024 rent'),
-- 2025: $1,825/mo (missed: Oct 2025)
(61, 2, 1825.00, '2025-01-04', 'January 2025 rent'),
(62, 2, 1825.00, '2025-02-05', 'February 2025 rent'),
(63, 2, 1825.00, '2025-03-04', 'March 2025 rent'),
(64, 2, 1825.00, '2025-04-03', 'April 2025 rent'),
(65, 2, 1825.00, '2025-05-03', 'May 2025 rent'),
(66, 2, 1825.00, '2025-06-04', 'June 2025 rent'),
(67, 2, 1825.00, '2025-07-03', 'July 2025 rent'),
(68, 2, 1825.00, '2025-08-05', 'August 2025 rent'),
(69, 2, 1825.00, '2025-09-04', 'September 2025 rent'),
-- October 2025 missed
(70, 2, 1825.00, '2025-11-05', 'November 2025 rent'),
(71, 2, 1825.00, '2025-12-04', 'December 2025 rent'),
-- 2026: $1,825/mo (no increase)
(72, 2, 1825.00, '2026-01-03', 'January 2026 rent'),
(73, 2, 1825.00, '2026-02-04', 'February 2026 rent'),
(74, 2, 1825.00, '2026-03-04', 'March 2026 rent'),

-- Property 3 — Champaign Townhome
-- 2023: $1,300/mo
(75, 3, 1300.00, '2023-01-05', 'January 2023 rent'),
(76, 3, 1300.00, '2023-02-04', 'February 2023 rent'),
(77, 3, 1300.00, '2023-03-04', 'March 2023 rent'),
(78, 3, 1300.00, '2023-04-05', 'April 2023 rent'),
(79, 3, 1300.00, '2023-05-04', 'May 2023 rent'),
(80, 3, 1300.00, '2023-06-03', 'June 2023 rent'),
(81, 3, 1300.00, '2023-07-05', 'July 2023 rent'),
(82, 3, 1300.00, '2023-08-04', 'August 2023 rent'),
(83, 3, 1300.00, '2023-09-06', 'September 2023 rent'),
(84, 3, 1300.00, '2023-10-04', 'October 2023 rent'),
(85, 3, 1300.00, '2023-11-03', 'November 2023 rent'),
(86, 3, 1300.00, '2023-12-05', 'December 2023 rent'),
-- 2024: $1,375/mo (missed: Mar 2024)
(87,  3, 1375.00, '2024-01-04', 'January 2024 rent'),
(88,  3, 1375.00, '2024-02-03', 'February 2024 rent'),
-- March 2024 missed
(89,  3, 2750.00, '2024-04-05', 'April 2024 rent + March back payment'),
(90,  3, 1375.00, '2024-05-04', 'May 2024 rent'),
(91,  3, 1375.00, '2024-06-05', 'June 2024 rent'),
(92,  3, 1375.00, '2024-07-05', 'July 2024 rent'),
(93,  3, 1375.00, '2024-08-03', 'August 2024 rent'),
(94,  3, 1375.00, '2024-09-05', 'September 2024 rent'),
(95,  3, 1375.00, '2024-10-04', 'October 2024 rent'),
(96,  3, 1375.00, '2024-11-04', 'November 2024 rent'),
(97,  3, 1375.00, '2024-12-04', 'December 2024 rent'),
-- 2025: $1,450/mo (missed: Jul 2025)
(98,  3, 1450.00, '2025-01-04', 'January 2025 rent'),
(99,  3, 1450.00, '2025-02-05', 'February 2025 rent'),
(100, 3, 1450.00, '2025-03-04', 'March 2025 rent'),
(101, 3, 1450.00, '2025-04-03', 'April 2025 rent'),
(102, 3, 1450.00, '2025-05-03', 'May 2025 rent'),
(103, 3, 1450.00, '2025-06-04', 'June 2025 rent'),
-- July 2025 missed
(104, 3, 1450.00, '2025-08-05', 'August 2025 rent'),
(105, 3, 1450.00, '2025-09-04', 'September 2025 rent'),
(106, 3, 1450.00, '2025-10-03', 'October 2025 rent'),
(107, 3, 1450.00, '2025-11-05', 'November 2025 rent'),
(108, 3, 1450.00, '2025-12-04', 'December 2025 rent'),
-- 2026: $1,450/mo (no increase)
(109, 3, 1450.00, '2026-01-03', 'January 2026 rent'),
(110, 3, 1450.00, '2026-02-04', 'February 2026 rent'),
(111, 3, 1450.00, '2026-03-04', 'March 2026 rent');


-- ============================================================
-- EXPENSES
-- ~66 repair/insurance/utilities/supplies records
-- + 117 monthly mortgage records (IDs 67-183)
--
-- Mortgage amounts:
--   Property 1 (Lafayette Condo):      $820/mo  — Citizens Bank
--   Property 2 (West Lafayette House): $1,180/mo — Horizon Bank
--   Property 3 (Champaign Townhome):   $940/mo  — Heartland Credit Union
-- ============================================================

INSERT INTO expenses (expense_id, property_id, amount, date, category, vendor, description) VALUES

-- Property 1 — Lafayette Condo
(1,  1,  875.00, '2023-01-18', 'Insurance',  'Allstate',                   'Annual landlord insurance policy renewal'),
(2,  1,   95.00, '2023-02-11', 'Repair',     'Hoosier Plumbing Co.',       'Dripping bathroom faucet repair'),
(3,  1,  210.00, '2023-04-22', 'Repair',     'Ace Hardware / DIY',         'Replaced interior door hardware and deadbolt'),
(4,  1,  130.00, '2023-06-08', 'Utilities',  'Duke Energy',                'Electric bill — vacant month between tenants'),
(5,  1,  450.00, '2023-07-14', 'Repair',     'CoolAir HVAC Services',      'AC tune-up and refrigerant recharge'),
(6,  1,   60.00, '2023-09-19', 'Supplies',   'Menards',                    'Cleaning supplies and touch-up paint for turnover'),
(7,  1,  175.00, '2023-10-05', 'Repair',     'Hoosier Plumbing Co.',       'Running toilet rebuild — parts and labor'),
(8,  1,  900.00, '2023-12-02', 'Insurance',  'Allstate',                   'Annual landlord insurance renewal'),
(9,  1,  320.00, '2024-01-17', 'Repair',     'CoolAir HVAC Services',      'Furnace inspection and igniter replacement'),
(10, 1,   85.00, '2024-03-09', 'Supplies',   'Menards',                    'Caulk, weather stripping, and door sweep'),
(11, 1,  140.00, '2024-05-22', 'Repair',     'Ace Hardware / DIY',         'Patched drywall and repainted living room wall'),
(12, 1,  220.00, '2024-07-11', 'Repair',     'CoolAir HVAC Services',      'AC capacitor replacement'),
(13, 1,   75.00, '2024-08-30', 'Supplies',   'Home Depot',                 'Bathroom caulk and grout re-seal'),
(14, 1,  900.00, '2024-11-30', 'Insurance',  'Allstate',                   'Annual landlord insurance renewal'),
(15, 1,  185.00, '2025-02-14', 'Repair',     'Hoosier Plumbing Co.',       'Kitchen sink drain clog — snaked and cleared'),
(16, 1,  430.00, '2025-04-03', 'Repair',     'Lafayette Appliance Repair', 'Dishwasher pump replacement'),
(17, 1,  115.00, '2025-06-18', 'Supplies',   'Menards',                    'Lightbulb replacement and misc hardware'),
(18, 1,  260.00, '2025-08-07', 'Repair',     'CoolAir HVAC Services',      'Air handler cleaning and coil treatment'),
(19, 1,  925.00, '2025-11-29', 'Insurance',  'Allstate',                   'Annual landlord insurance renewal'),
(20, 1,  195.00, '2026-01-10', 'Repair',     'Hoosier Plumbing Co.',       'Water heater anode rod replacement'),
(21, 1,   90.00, '2026-02-20', 'Supplies',   'Menards',                    'Smoke detector batteries and CO alarm replacement'),

-- Property 2 — West Lafayette House
(22, 2, 1100.00, '2023-01-06', 'Insurance',  'State Farm',                 'Annual landlord insurance policy'),
(23, 2,  380.00, '2023-02-23', 'Repair',     'Tippecanoe Roofing',         'Repaired flashing around chimney after ice dam'),
(24, 2,  145.00, '2023-03-15', 'Utilities',  'Indiana American Water',     'Water leak adjustment — frozen pipe burst'),
(25, 2,  550.00, '2023-03-16', 'Repair',     'Hoosier Plumbing Co.',       'Emergency frozen pipe repair — laundry room'),
(26, 2,   95.00, '2023-05-10', 'Supplies',   'Home Depot',                 'Deck stain and brushes for spring touch-up'),
(27, 2,  200.00, '2023-06-28', 'Repair',     'West Lafayette Lawn Care',   'Lawn aeration and overseeding'),
(28, 2,  640.00, '2023-09-14', 'Repair',     'CoolAir HVAC Services',      'Replaced blower motor in furnace'),
(29, 2, 1100.00, '2023-12-05', 'Insurance',  'State Farm',                 'Annual landlord insurance renewal'),
(30, 2,  175.00, '2024-01-25', 'Repair',     'Tippecanoe Roofing',         'Replaced three damaged shingles — storm damage'),
(31, 2,  310.00, '2024-03-20', 'Repair',     'Hoosier Plumbing Co.',       'Replaced shutoff valves under kitchen sink'),
(32, 2,   80.00, '2024-04-12', 'Supplies',   'Menards',                    'Gutter guards installed — front and back'),
(33, 2,  495.00, '2024-06-02', 'Repair',     'West Lafayette Lawn Care',   'Removed dead tree from backyard'),
(34, 2,  220.00, '2024-07-19', 'Repair',     'Lafayette Appliance Repair', 'Dryer heating element replacement'),
(35, 2,  135.00, '2024-09-06', 'Supplies',   'Home Depot',                 'Interior paint — hallway touch-up and trim'),
(36, 2, 1125.00, '2024-12-01', 'Insurance',  'State Farm',                 'Annual landlord insurance renewal'),
(37, 2,  750.00, '2025-01-22', 'Repair',     'CoolAir HVAC Services',      'Full furnace service and heat exchanger inspection'),
(38, 2,  160.00, '2025-03-11', 'Repair',     'Tippecanoe Roofing',         'Patched small roof leak above master bedroom'),
(39, 2,  285.00, '2025-05-08', 'Repair',     'West Lafayette Lawn Care',   'Spring cleanup, edging, and mulch install'),
(40, 2,   70.00, '2025-06-25', 'Supplies',   'Menards',                    'Window screen replacements — three windows'),
(41, 2,  410.00, '2025-08-13', 'Repair',     'Hoosier Plumbing Co.',       'Replaced exterior hose bib — cracked housing'),
(42, 2,  195.00, '2025-10-17', 'Repair',     'Lafayette Appliance Repair', 'Refrigerator ice maker repair'),
(43, 2, 1150.00, '2025-12-02', 'Insurance',  'State Farm',                 'Annual landlord insurance renewal'),
(44, 2,  240.00, '2026-01-15', 'Repair',     'CoolAir HVAC Services',      'Humidifier pad replacement and furnace tune-up'),
(45, 2,  110.00, '2026-02-27', 'Supplies',   'Home Depot',                 'Recaulked master bath tub surround'),

-- Property 3 — Champaign Townhome
(46, 3,  850.00, '2023-01-10', 'Insurance',  'Farmers Insurance',          'Annual landlord insurance policy'),
(47, 3,  290.00, '2023-02-17', 'Repair',     'Champaign Plumbing & Heat',  'Water heater pilot light and thermocouple repair'),
(48, 3,  120.00, '2023-04-06', 'Supplies',   'Menards',                    'Spring cleaning supplies and patio door lubricant'),
(49, 3,  480.00, '2023-05-24', 'Repair',     'Illini HVAC Solutions',      'AC condenser cleaning and refrigerant top-off'),
(50, 3,  165.00, '2023-08-09', 'Repair',     'Champaign Plumbing & Heat',  'Garbage disposal replacement — kitchen'),
(51, 3,   90.00, '2023-09-28', 'Supplies',   'Home Depot',                 'Painted bedroom and closet — tenant turnover'),
(52, 3,  870.00, '2023-12-08', 'Insurance',  'Farmers Insurance',          'Annual landlord insurance renewal'),
(53, 3,  340.00, '2024-01-30', 'Repair',     'Illini HVAC Solutions',      'Furnace draft motor replacement'),
(54, 3,  175.00, '2024-03-14', 'Repair',     'Champaign Plumbing & Heat',  'Leaking supply line to toilet — replaced valve'),
(55, 3,  230.00, '2024-05-16', 'Repair',     'C-U Handyman Services',      'Fixed fence gate latch and rotted post section'),
(56, 3,  395.00, '2024-08-01', 'Repair',     'Illini HVAC Solutions',      'Thermostat upgrade to smart thermostat — installed'),
(57, 3,  105.00, '2024-09-20', 'Supplies',   'Menards',                    'Replaced bathroom vanity light fixture'),
(58, 3,  880.00, '2024-12-04', 'Insurance',  'Farmers Insurance',          'Annual landlord insurance renewal'),
(59, 3,  215.00, '2025-02-08', 'Repair',     'Champaign Plumbing & Heat',  'Slow drain snaking — bathroom tub'),
(60, 3,  460.00, '2025-04-23', 'Repair',     'Illini HVAC Solutions',      'AC evaporator coil cleaning — pre-season service'),
(61, 3,  145.00, '2025-06-11', 'Supplies',   'Home Depot',                 'Replaced kitchen faucet — tenant request'),
(62, 3,  320.00, '2025-08-22', 'Repair',     'C-U Handyman Services',      'Repaired cracked concrete on front walkway'),
(63, 3,  195.00, '2025-10-09', 'Repair',     'Champaign Plumbing & Heat',  'Water pressure regulator adjustment and test'),
(64, 3,  900.00, '2025-12-06', 'Insurance',  'Farmers Insurance',          'Annual landlord insurance renewal'),
(65, 3,  275.00, '2026-01-21', 'Repair',     'Illini HVAC Solutions',      'Annual furnace tune-up and filter replacement'),
(66, 3,   85.00, '2026-03-03', 'Supplies',   'Menards',                    'Touch-up paint and caulk — spring maintenance'),

-- ============================================================
-- MORTGAGE PAYMENTS
-- Property 1: $820/mo  — Citizens Bank         (IDs 67-105)
-- Property 2: $1,180/mo — Horizon Bank         (IDs 106-144)
-- Property 3: $940/mo  — Heartland Credit Union (IDs 145-183)
-- ============================================================

-- Property 1 — Lafayette Condo mortgage
(67,  1,  820.00, '2023-01-01', 'Mortgage', 'Citizens Bank', 'January 2023 mortgage payment'),
(68,  1,  820.00, '2023-02-01', 'Mortgage', 'Citizens Bank', 'February 2023 mortgage payment'),
(69,  1,  820.00, '2023-03-01', 'Mortgage', 'Citizens Bank', 'March 2023 mortgage payment'),
(70,  1,  820.00, '2023-04-01', 'Mortgage', 'Citizens Bank', 'April 2023 mortgage payment'),
(71,  1,  820.00, '2023-05-01', 'Mortgage', 'Citizens Bank', 'May 2023 mortgage payment'),
(72,  1,  820.00, '2023-06-01', 'Mortgage', 'Citizens Bank', 'June 2023 mortgage payment'),
(73,  1,  820.00, '2023-07-01', 'Mortgage', 'Citizens Bank', 'July 2023 mortgage payment'),
(74,  1,  820.00, '2023-08-01', 'Mortgage', 'Citizens Bank', 'August 2023 mortgage payment'),
(75,  1,  820.00, '2023-09-01', 'Mortgage', 'Citizens Bank', 'September 2023 mortgage payment'),
(76,  1,  820.00, '2023-10-01', 'Mortgage', 'Citizens Bank', 'October 2023 mortgage payment'),
(77,  1,  820.00, '2023-11-01', 'Mortgage', 'Citizens Bank', 'November 2023 mortgage payment'),
(78,  1,  820.00, '2023-12-01', 'Mortgage', 'Citizens Bank', 'December 2023 mortgage payment'),
(79,  1,  820.00, '2024-01-01', 'Mortgage', 'Citizens Bank', 'January 2024 mortgage payment'),
(80,  1,  820.00, '2024-02-01', 'Mortgage', 'Citizens Bank', 'February 2024 mortgage payment'),
(81,  1,  820.00, '2024-03-01', 'Mortgage', 'Citizens Bank', 'March 2024 mortgage payment'),
(82,  1,  820.00, '2024-04-01', 'Mortgage', 'Citizens Bank', 'April 2024 mortgage payment'),
(83,  1,  820.00, '2024-05-01', 'Mortgage', 'Citizens Bank', 'May 2024 mortgage payment'),
(84,  1,  820.00, '2024-06-01', 'Mortgage', 'Citizens Bank', 'June 2024 mortgage payment'),
(85,  1,  820.00, '2024-07-01', 'Mortgage', 'Citizens Bank', 'July 2024 mortgage payment'),
(86,  1,  820.00, '2024-08-01', 'Mortgage', 'Citizens Bank', 'August 2024 mortgage payment'),
(87,  1,  820.00, '2024-09-01', 'Mortgage', 'Citizens Bank', 'September 2024 mortgage payment'),
(88,  1,  820.00, '2024-10-01', 'Mortgage', 'Citizens Bank', 'October 2024 mortgage payment'),
(89,  1,  820.00, '2024-11-01', 'Mortgage', 'Citizens Bank', 'November 2024 mortgage payment'),
(90,  1,  820.00, '2024-12-01', 'Mortgage', 'Citizens Bank', 'December 2024 mortgage payment'),
(91,  1,  820.00, '2025-01-01', 'Mortgage', 'Citizens Bank', 'January 2025 mortgage payment'),
(92,  1,  820.00, '2025-02-01', 'Mortgage', 'Citizens Bank', 'February 2025 mortgage payment'),
(93,  1,  820.00, '2025-03-01', 'Mortgage', 'Citizens Bank', 'March 2025 mortgage payment'),
(94,  1,  820.00, '2025-04-01', 'Mortgage', 'Citizens Bank', 'April 2025 mortgage payment'),
(95,  1,  820.00, '2025-05-01', 'Mortgage', 'Citizens Bank', 'May 2025 mortgage payment'),
(96,  1,  820.00, '2025-06-01', 'Mortgage', 'Citizens Bank', 'June 2025 mortgage payment'),
(97,  1,  820.00, '2025-07-01', 'Mortgage', 'Citizens Bank', 'July 2025 mortgage payment'),
(98,  1,  820.00, '2025-08-01', 'Mortgage', 'Citizens Bank', 'August 2025 mortgage payment'),
(99,  1,  820.00, '2025-09-01', 'Mortgage', 'Citizens Bank', 'September 2025 mortgage payment'),
(100, 1,  820.00, '2025-10-01', 'Mortgage', 'Citizens Bank', 'October 2025 mortgage payment'),
(101, 1,  820.00, '2025-11-01', 'Mortgage', 'Citizens Bank', 'November 2025 mortgage payment'),
(102, 1,  820.00, '2025-12-01', 'Mortgage', 'Citizens Bank', 'December 2025 mortgage payment'),
(103, 1,  820.00, '2026-01-01', 'Mortgage', 'Citizens Bank', 'January 2026 mortgage payment'),
(104, 1,  820.00, '2026-02-01', 'Mortgage', 'Citizens Bank', 'February 2026 mortgage payment'),
(105, 1,  820.00, '2026-03-01', 'Mortgage', 'Citizens Bank', 'March 2026 mortgage payment'),

-- Property 2 — West Lafayette House mortgage
(106, 2, 1180.00, '2023-01-01', 'Mortgage', 'Horizon Bank', 'January 2023 mortgage payment'),
(107, 2, 1180.00, '2023-02-01', 'Mortgage', 'Horizon Bank', 'February 2023 mortgage payment'),
(108, 2, 1180.00, '2023-03-01', 'Mortgage', 'Horizon Bank', 'March 2023 mortgage payment'),
(109, 2, 1180.00, '2023-04-01', 'Mortgage', 'Horizon Bank', 'April 2023 mortgage payment'),
(110, 2, 1180.00, '2023-05-01', 'Mortgage', 'Horizon Bank', 'May 2023 mortgage payment'),
(111, 2, 1180.00, '2023-06-01', 'Mortgage', 'Horizon Bank', 'June 2023 mortgage payment'),
(112, 2, 1180.00, '2023-07-01', 'Mortgage', 'Horizon Bank', 'July 2023 mortgage payment'),
(113, 2, 1180.00, '2023-08-01', 'Mortgage', 'Horizon Bank', 'August 2023 mortgage payment'),
(114, 2, 1180.00, '2023-09-01', 'Mortgage', 'Horizon Bank', 'September 2023 mortgage payment'),
(115, 2, 1180.00, '2023-10-01', 'Mortgage', 'Horizon Bank', 'October 2023 mortgage payment'),
(116, 2, 1180.00, '2023-11-01', 'Mortgage', 'Horizon Bank', 'November 2023 mortgage payment'),
(117, 2, 1180.00, '2023-12-01', 'Mortgage', 'Horizon Bank', 'December 2023 mortgage payment'),
(118, 2, 1180.00, '2024-01-01', 'Mortgage', 'Horizon Bank', 'January 2024 mortgage payment'),
(119, 2, 1180.00, '2024-02-01', 'Mortgage', 'Horizon Bank', 'February 2024 mortgage payment'),
(120, 2, 1180.00, '2024-03-01', 'Mortgage', 'Horizon Bank', 'March 2024 mortgage payment'),
(121, 2, 1180.00, '2024-04-01', 'Mortgage', 'Horizon Bank', 'April 2024 mortgage payment'),
(122, 2, 1180.00, '2024-05-01', 'Mortgage', 'Horizon Bank', 'May 2024 mortgage payment'),
(123, 2, 1180.00, '2024-06-01', 'Mortgage', 'Horizon Bank', 'June 2024 mortgage payment'),
(124, 2, 1180.00, '2024-07-01', 'Mortgage', 'Horizon Bank', 'July 2024 mortgage payment'),
(125, 2, 1180.00, '2024-08-01', 'Mortgage', 'Horizon Bank', 'August 2024 mortgage payment'),
(126, 2, 1180.00, '2024-09-01', 'Mortgage', 'Horizon Bank', 'September 2024 mortgage payment'),
(127, 2, 1180.00, '2024-10-01', 'Mortgage', 'Horizon Bank', 'October 2024 mortgage payment'),
(128, 2, 1180.00, '2024-11-01', 'Mortgage', 'Horizon Bank', 'November 2024 mortgage payment'),
(129, 2, 1180.00, '2024-12-01', 'Mortgage', 'Horizon Bank', 'December 2024 mortgage payment'),
(130, 2, 1180.00, '2025-01-01', 'Mortgage', 'Horizon Bank', 'January 2025 mortgage payment'),
(131, 2, 1180.00, '2025-02-01', 'Mortgage', 'Horizon Bank', 'February 2025 mortgage payment'),
(132, 2, 1180.00, '2025-03-01', 'Mortgage', 'Horizon Bank', 'March 2025 mortgage payment'),
(133, 2, 1180.00, '2025-04-01', 'Mortgage', 'Horizon Bank', 'April 2025 mortgage payment'),
(134, 2, 1180.00, '2025-05-01', 'Mortgage', 'Horizon Bank', 'May 2025 mortgage payment'),
(135, 2, 1180.00, '2025-06-01', 'Mortgage', 'Horizon Bank', 'June 2025 mortgage payment'),
(136, 2, 1180.00, '2025-07-01', 'Mortgage', 'Horizon Bank', 'July 2025 mortgage payment'),
(137, 2, 1180.00, '2025-08-01', 'Mortgage', 'Horizon Bank', 'August 2025 mortgage payment'),
(138, 2, 1180.00, '2025-09-01', 'Mortgage', 'Horizon Bank', 'September 2025 mortgage payment'),
(139, 2, 1180.00, '2025-10-01', 'Mortgage', 'Horizon Bank', 'October 2025 mortgage payment'),
(140, 2, 1180.00, '2025-11-01', 'Mortgage', 'Horizon Bank', 'November 2025 mortgage payment'),
(141, 2, 1180.00, '2025-12-01', 'Mortgage', 'Horizon Bank', 'December 2025 mortgage payment'),
(142, 2, 1180.00, '2026-01-01', 'Mortgage', 'Horizon Bank', 'January 2026 mortgage payment'),
(143, 2, 1180.00, '2026-02-01', 'Mortgage', 'Horizon Bank', 'February 2026 mortgage payment'),
(144, 2, 1180.00, '2026-03-01', 'Mortgage', 'Horizon Bank', 'March 2026 mortgage payment'),

-- Property 3 — Champaign Townhome mortgage
(145, 3,  940.00, '2023-01-01', 'Mortgage', 'Heartland Credit Union', 'January 2023 mortgage payment'),
(146, 3,  940.00, '2023-02-01', 'Mortgage', 'Heartland Credit Union', 'February 2023 mortgage payment'),
(147, 3,  940.00, '2023-03-01', 'Mortgage', 'Heartland Credit Union', 'March 2023 mortgage payment'),
(148, 3,  940.00, '2023-04-01', 'Mortgage', 'Heartland Credit Union', 'April 2023 mortgage payment'),
(149, 3,  940.00, '2023-05-01', 'Mortgage', 'Heartland Credit Union', 'May 2023 mortgage payment'),
(150, 3,  940.00, '2023-06-01', 'Mortgage', 'Heartland Credit Union', 'June 2023 mortgage payment'),
(151, 3,  940.00, '2023-07-01', 'Mortgage', 'Heartland Credit Union', 'July 2023 mortgage payment'),
(152, 3,  940.00, '2023-08-01', 'Mortgage', 'Heartland Credit Union', 'August 2023 mortgage payment'),
(153, 3,  940.00, '2023-09-01', 'Mortgage', 'Heartland Credit Union', 'September 2023 mortgage payment'),
(154, 3,  940.00, '2023-10-01', 'Mortgage', 'Heartland Credit Union', 'October 2023 mortgage payment'),
(155, 3,  940.00, '2023-11-01', 'Mortgage', 'Heartland Credit Union', 'November 2023 mortgage payment'),
(156, 3,  940.00, '2023-12-01', 'Mortgage', 'Heartland Credit Union', 'December 2023 mortgage payment'),
(157, 3,  940.00, '2024-01-01', 'Mortgage', 'Heartland Credit Union', 'January 2024 mortgage payment'),
(158, 3,  940.00, '2024-02-01', 'Mortgage', 'Heartland Credit Union', 'February 2024 mortgage payment'),
(159, 3,  940.00, '2024-03-01', 'Mortgage', 'Heartland Credit Union', 'March 2024 mortgage payment'),
(160, 3,  940.00, '2024-04-01', 'Mortgage', 'Heartland Credit Union', 'April 2024 mortgage payment'),
(161, 3,  940.00, '2024-05-01', 'Mortgage', 'Heartland Credit Union', 'May 2024 mortgage payment'),
(162, 3,  940.00, '2024-06-01', 'Mortgage', 'Heartland Credit Union', 'June 2024 mortgage payment'),
(163, 3,  940.00, '2024-07-01', 'Mortgage', 'Heartland Credit Union', 'July 2024 mortgage payment'),
(164, 3,  940.00, '2024-08-01', 'Mortgage', 'Heartland Credit Union', 'August 2024 mortgage payment'),
(165, 3,  940.00, '2024-09-01', 'Mortgage', 'Heartland Credit Union', 'September 2024 mortgage payment'),
(166, 3,  940.00, '2024-10-01', 'Mortgage', 'Heartland Credit Union', 'October 2024 mortgage payment'),
(167, 3,  940.00, '2024-11-01', 'Mortgage', 'Heartland Credit Union', 'November 2024 mortgage payment'),
(168, 3,  940.00, '2024-12-01', 'Mortgage', 'Heartland Credit Union', 'December 2024 mortgage payment'),
(169, 3,  940.00, '2025-01-01', 'Mortgage', 'Heartland Credit Union', 'January 2025 mortgage payment'),
(170, 3,  940.00, '2025-02-01', 'Mortgage', 'Heartland Credit Union', 'February 2025 mortgage payment'),
(171, 3,  940.00, '2025-03-01', 'Mortgage', 'Heartland Credit Union', 'March 2025 mortgage payment'),
(172, 3,  940.00, '2025-04-01', 'Mortgage', 'Heartland Credit Union', 'April 2025 mortgage payment'),
(173, 3,  940.00, '2025-05-01', 'Mortgage', 'Heartland Credit Union', 'May 2025 mortgage payment'),
(174, 3,  940.00, '2025-06-01', 'Mortgage', 'Heartland Credit Union', 'June 2025 mortgage payment'),
(175, 3,  940.00, '2025-07-01', 'Mortgage', 'Heartland Credit Union', 'July 2025 mortgage payment'),
(176, 3,  940.00, '2025-08-01', 'Mortgage', 'Heartland Credit Union', 'August 2025 mortgage payment'),
(177, 3,  940.00, '2025-09-01', 'Mortgage', 'Heartland Credit Union', 'September 2025 mortgage payment'),
(178, 3,  940.00, '2025-10-01', 'Mortgage', 'Heartland Credit Union', 'October 2025 mortgage payment'),
(179, 3,  940.00, '2025-11-01', 'Mortgage', 'Heartland Credit Union', 'November 2025 mortgage payment'),
(180, 3,  940.00, '2025-12-01', 'Mortgage', 'Heartland Credit Union', 'December 2025 mortgage payment'),
(181, 3,  940.00, '2026-01-01', 'Mortgage', 'Heartland Credit Union', 'January 2026 mortgage payment'),
(182, 3,  940.00, '2026-02-01', 'Mortgage', 'Heartland Credit Union', 'February 2026 mortgage payment'),
(183, 3,  940.00, '2026-03-01', 'Mortgage', 'Heartland Credit Union', 'March 2026 mortgage payment');

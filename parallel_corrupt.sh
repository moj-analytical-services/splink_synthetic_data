
#!/bin/zsh
set -e
set -o pipefail



python  07_corrupt_records.py --start_year=1550 --num_years=10 &
python  07_corrupt_records.py --start_year=1560 --num_years=10 &
python  07_corrupt_records.py --start_year=1570 --num_years=10 &
python  07_corrupt_records.py --start_year=1580 --num_years=10 &
python  07_corrupt_records.py --start_year=1590 --num_years=10 &
python  07_corrupt_records.py --start_year=1600 --num_years=10 &
python  07_corrupt_records.py --start_year=1610 --num_years=10 &
python  07_corrupt_records.py --start_year=1620 --num_years=10 &
python  07_corrupt_records.py --start_year=1630 --num_years=10 &
python  07_corrupt_records.py --start_year=1640 --num_years=10 &
python  07_corrupt_records.py --start_year=1650 --num_years=10 &
python  07_corrupt_records.py --start_year=1660 --num_years=10 &
python  07_corrupt_records.py --start_year=1670 --num_years=10 &
python  07_corrupt_records.py --start_year=1680 --num_years=10 &




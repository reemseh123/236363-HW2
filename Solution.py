from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

# disks_ram_enhanced = pairs of (ram.id,disk.id)
# saved_files = pairs of (file.id,disk.id)
# saved_files_file_details = details of saved files only
# saved_files_disk_details = details of used(contains at least one file) disks only
# disks_ram_enhanced_ram_details =
# disks_ram_enhanced_disk_details =


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = """
        CREATE TABLE files(
            file_id INTEGER PRIMARY KEY CHECK (file_id > 0),
            type TEXT NOT NULL,
            size INTEGER NOT NULL CHECK (size >= 0)
         );
        CREATE TABLE disks(
            disk_id INTEGER PRIMARY KEY CHECK (disk_id > 0),
            manufacturing_company TEXT NOT NULL,
            speed INTEGER NOT NULL CHECK (speed > 0),
            free_space INTEGER NOT NULL CHECK (free_space >= 0),
            cost_per_byte INTEGER NOT NULL CHECK (cost_per_byte > 0)
         );
        CREATE TABLE rams(
            ram_id INTEGER PRIMARY KEY CHECK (ram_id > 0),
            company TEXT NOT NULL,
            size INTEGER NOT NULL CHECK (size > 0)
         );
        CREATE TABLE saved_files(
            file_id INTEGER,
            disk_id INTEGER,
            FOREIGN KEY (file_id) 
            REFERENCES files(file_id)
            ON DELETE CASCADE, 
            FOREIGN KEY (disk_id) 
            REFERENCES disks(disk_id)
            ON DELETE CASCADE,
            PRIMARY KEY(file_id, disk_id)
        );
        CREATE TABLE disks_ram_enhanced(
            ram_id INTEGER,
            disk_id INTEGER,
            FOREIGN KEY (ram_id) 
            REFERENCES rams(ram_id)
            ON DELETE CASCADE, 
            FOREIGN KEY (disk_id) 
            REFERENCES disks(disk_id)
            ON DELETE CASCADE,
            PRIMARY KEY(ram_id, disk_id)
        );

        CREATE VIEW saved_files_file_details AS 
        SELECT saved_files.disk_id, files.*
        FROM saved_files 
        INNER JOIN files 
        ON saved_files.file_id = files.file_id;

        CREATE VIEW saved_files_disk_details AS 
        SELECT saved_files.file_id, disks.*
        FROM saved_files 
        INNER JOIN disks 
        ON saved_files.disk_id = disks.disk_id;

        CREATE VIEW disks_ram_enhanced_ram_details AS 
        SELECT disks_ram_enhanced.disk_id, rams.*
        FROM disks_ram_enhanced 
        INNER JOIN rams 
        ON disks_ram_enhanced.ram_id = rams.ram_id;

        CREATE VIEW disks_ram_enhanced_disk_details AS 
        SELECT disks_ram_enhanced.ram_id, disks.*
        FROM disks_ram_enhanced 
        INNER JOIN disks 
        ON disks_ram_enhanced.disk_id = disks.disk_id;
        """
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    pass


def dropTables():
    pass


def addFile(file: File) -> Status:
    return Status.OK


def getFileByID(fileID: int) -> File:
    return File()


def deleteFile(file: File) -> Status:
    return Status.OK


def addDisk(disk: Disk) -> Status:
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    # add using query: file , then disk (assume they aren't exist)
    # catch errors with rollback.
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    # Insert into saved_files.(FK violation , UNIQUE violation)
    # update free space. (CHECK violation)
    # catch errors with rollback.
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    # remove from saved_files
    # update free space
    # catch errors
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    # Insert into disks_ram_enhanced
    # catch errors
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    # remove from disks_ram_enhanced
    # catch errors
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    # use the view saved_files_file_details
    # where this diskID.
    # and aggregate (AVG) (if empty will return NULL - we should convert to zero)
    # catch errors
    return 0


def diskTotalRAM(diskID: int) -> int:
    # use the view disks_ram_enhanced_ram_details
    # where this diskID.
    # and aggregate (SUM) (if empty will return NULL - we should convert to zero)
    # catch errors
    return 0


def getCostForType(type: str) -> int:
    # join view saved_files_file_details where this Type
    # with vew saved_files_disk_details where this Type
    # ON pair key (file_id,disk_id)
    # where Type
    # then agg (SUM)(if empty will return NULL - we should convert to zero)
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    # FILES ORDERED By SIZE THEN BY ID'S
    # where size <= freespace of this disk (sub-query)
    # LIMIT 5
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    # check if we can use "diskTotalRAM" func.
    # if not, then
    # FILES ORDERED By SIZE THEN BY ID'S
    # where size <= freespace of this disk (sub-query)
    # where size <= sum of... (same sub-query used in the func "diskTotalRAM")
    # LIMIT 5
    return []


def isCompanyExclusive(diskID: int) -> bool:
    # join the Ram VIEWS (SAME AS FILES VIEWS)
    #  where this disk_id
    # SELECT * ... where manufacturing_company != company
    # if the length of the result is zero then true.
    # WARNING : check the empty case
    # means when no rams associated with the disk.
    return True


def getConflictingDisks() -> List[int]:
    # SELECT DISTINCT a1.disk_id
    # FROM saved_files a1
    # INNER join saved_files a2
    # on a1.file_id = a2.file_id
    # WHERE a1.disk_id != a2.disk_id
    # ORDER by a1.disk_id ASC
    return []


def mostAvailableDisks() -> List[int]:
    # (nested queries -
    # using the subquery used in
    # getFilesCanBeAddedToDisk (with little modification - maybe)
    # without LIMIT)
    # SIMPLY BY FREE SPACE ( ORDER ACCORDINGLY)
    return []


def getCloseFiles(fileID: int) -> List[int]:
    # files where this file id
    # SELECT disk_id from saved_files where this file_id = relevant_disk_ids

    # join saved_files with relevant_disk_ids on disk_id
    # now we have table with files saved on the relevant disks.
    # group by file_id aggregate COUNT disk_id. = call it relevant_disks_count

   ##############################################
    # join relevant_disks_count with files on file id, project only file_id, countOFDisks.
    # convert NULLs to zeroes
    # order by ID
    # get top 10 with the condition >= x/2
    # when x is the result of sub-query that returns the disks count of this file_id.

    return []
    # where (after grouping) where count > 50% ... (exclude this file_id)
    # in case the file is not saved - return top 10 from files.
    # (we can relate to the above as subquery, the ask if exists...)

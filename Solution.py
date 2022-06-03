from typing import List
import hw2_spring2022.Utility.DBConnector as Connector
from hw2_spring2022.Utility.Status import Status
from hw2_spring2022.Utility.Exceptions import DatabaseException
from hw2_spring2022.Business.File import File
from hw2_spring2022.Business.RAM import RAM
from hw2_spring2022.Business.Disk import Disk
from psycopg2 import sql


# disks_ram_enhanced = pairs of (ram.id,disk.id)
# saved_files = pairs of (file.id,disk.id)
# saved_files_file_details = details of saved files only
# saved_files_disk_details = details of used(contains at least one file) disks only
# disks_ram_enhanced_ram_details =
# disks_ram_enhanced_disk_details =

def mapToFile(fileID: int, type: str, size: int) -> File:
    return File(fileID, type, size)


def mapToRam(ramID: int, company: str, size: int) -> RAM:
    return RAM(ramID, company, size)


def mapToDisk(diskID: int, company: str, speed: int, free_space: int, cost: int) -> Disk:
    return Disk(diskID, company, speed, free_space, cost)


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = """
                CREATE TABLE IF NOT EXISTS files(
                    file_id INTEGER PRIMARY KEY CHECK (file_id > 0),
                    type TEXT NOT NULL,
                    size INTEGER NOT NULL CHECK (size >= 0)
                 );
                CREATE TABLE IF NOT EXISTS disks(
                    disk_id INTEGER PRIMARY KEY CHECK (disk_id > 0),
                    manufacturing_company TEXT NOT NULL,
                    speed INTEGER NOT NULL CHECK (speed > 0),
                    free_space INTEGER NOT NULL CHECK (free_space >= 0),
                    cost_per_byte INTEGER NOT NULL CHECK (cost_per_byte > 0)
                 );
                CREATE TABLE IF NOT EXISTS rams(
                    ram_id INTEGER PRIMARY KEY CHECK (ram_id > 0),
                    company TEXT NOT NULL,
                    size INTEGER NOT NULL CHECK (size > 0)
                 );
                CREATE TABLE IF NOT EXISTS saved_files(
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
                CREATE TABLE IF NOT EXISTS disks_ram_enhanced(
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
                
                CREATE VIEW IF NOT EXISTS saved_files_file_details AS 
                SELECT saved_files.disk_id, files.* 
                FROM saved_files 
                INNER JOIN files 
                ON saved_files.file_id = files.file_id;
                
                CREATE VIEW IF NOT EXISTS saved_files_disk_details AS 
                SELECT saved_files.file_id, disks.* 
                FROM saved_files 
                INNER JOIN disks 
                ON saved_files.disk_id = disks.disk_id;
                
                CREATE VIEW IF NOT EXISTS disks_ram_enhanced_ram_details AS 
                SELECT disks_ram_enhanced.disk_id, rams.* 
                FROM disks_ram_enhanced 
                INNER JOIN rams 
                ON disks_ram_enhanced.ram_id = rams.ram_id;
                
                CREATE VIEW IF NOT EXISTS disks_ram_enhanced_disk_details AS 
                SELECT disks_ram_enhanced.ram_id, disks.* 
                FROM disks_ram_enhanced 
                INNER JOIN disks 
                ON disks_ram_enhanced.disk_id = disks.disk_id;
                
                CREATE VIEW IF NOT EXISTS rams_And_Disks_Details AS 
                SELECT rDetails.ram_id,rDetails.disk_id,rDetails.company AS ram_company, dDetails.manufacturing_company AS disk_company 
                FROM disks_ram_enhanced_ram_details rDetails INNER
                JOIN disks_ram_enhanced_disk_details dDetails 
                ON rDetails.ram_id = dDetails.ram_id 
                AND rDetails.disk_id = dDetails.disk_id ;
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
    conn = None
    try:
        conn = Connector.DBConnector()
        query = """
        DELETE FROM files;   
        DELETE FROM disks;
        DELETE FROM rams;
        """
        conn.execute(query)
        conn.commit()
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = """
                        DROP VIEW IF EXISTS saved_files_file_details;
                        DROP VIEW IF EXISTS saved_files_disk_details;
                        DROP VIEW IF EXISTS rams_And_Disks_Details;
                        DROP VIEW IF EXISTS disks_ram_enhanced_ram_details;
                        DROP VIEW IF EXISTS disks_ram_enhanced_disk_details;
                        DROP TABLE IF EXISTS disks_ram_enhanced;
                        DROP TABLE IF EXISTS saved_files;
                        DROP TABLE IF EXISTS files;
                        DROP TABLE IF EXISTS disks;
                        DROP TABLE IF EXISTS rams;
                        """
        conn.execute(query)
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
    pass


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
                INSERT INTO files(file_id,type,size)  
                VALUES({id},{type},{size}) 
            """
        ).format(
            id=sql.Literal(file.getFileID()),
            type=sql.Literal(file.getType()),
            size=sql.Literal(file.getSize())
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except Exception as e:
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def getFileByID(fileID: int) -> File:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM files where file_id={id} "
        ).format(id=sql.Literal(fileID))
        conn.commit()
        res, result = conn.execute(query)
    except Exception as e:
        return File.badFile()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    if res == 0:
        return File.badFile()
    return mapToFile(
        result[0]["file_id"],
        result[0]["type"],
        result[0]["size"],
    )


def deleteFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            UPDATE disks SET free_space=(free_space+{size})
             WHERE  
             EXISTS (
                 SELECT * FROM saved_files WHERE disk_id IN (SELECT disk_id FROM saved_files WHERE file_id={id}) 
             );
            DELETE FROM files WHERE file_id={id};
            """
        ).format(
            id=sql.Literal(file.getFileID()),
            size=sql.Literal(file.getSize())
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            INSERT INTO disks(disk_id,manufacturing_company,speed,free_space,cost_per_byte)  
            VALUES({id},{company},{speed},{freeSpace},{cost}) 
            """
        ).format(
            id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            freeSpace=sql.Literal(disk.getFreeSpace()),
            cost=sql.Literal(disk.getCost())
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except Exception as e:
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM disks where disk_id={id} ").format(id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Disk.badDisk()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    if rows_effected == 0:
        return Disk.badDisk()
    return mapToDisk(
        result[0]["disk_id"],
        result[0]["manufacturing_company"],
        result[0]["speed"],
        result[0]["free_space"],
        result[0]["cost_per_byte"],
    )


# not sure about this
#   query = sql.SQL("INSERT INTO Users(id, name) VALUES({id}, {username})").format(id=sql.Literal(ID)
#   username=sql.Literal(name))

# query = sql.SQL("DELETE FROM disks WHERE diskID = {0}").format(id=sql.Literal(diskID))#


def deleteDisk(diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT * FROM disks WHERE disk_id={id};
            DELETE FROM disks WHERE disk_id={id};
            """
        ).format(
            id=sql.Literal(diskID)
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO rams(ram_id,company,size) VALUES({id},{company},{size}) "
        ).format(
            id=sql.Literal(ram.getRamID()),
            company=sql.Literal(ram.getCompany()),
            size=sql.Literal(ram.getSize())
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except Exception as e:
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM rams where ram_id={id} ").format(id=sql.Literal(ramID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return RAM.badRAM()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    if rows_effected == 0:
        return RAM.badRAM()
    return mapToRam(
        result[0]["ram_id"],
        result[0]["company"],
        result[0]["size"],
    )


def deleteRAM(ramID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT * FROM rams WHERE ram_id={id}; 
            DELETE FROM rams WHERE ram_id={id};
            """
        ).format(
            id=sql.Literal(ramID)
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


# add using query: file , then disk (assume they aren't exist)
# catch errors with rollback.


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO disks(disk_id,manufacturing_company,speed,free_space,cost_per_byte) "
            "VALUES({id},{company},{speed},{freeSpace},{cost}); "
            "INSERT INTO files(file_id,type,size) "
            "VALUES({fId},{fType},{fSize}); "
        ).format(
            id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            freeSpace=sql.Literal(disk.getFreeSpace()),
            cost=sql.Literal(disk.getCost()),
            fId=sql.Literal(file.getFileID()),
            fType=sql.Literal(file.getType()),
            fSize=sql.Literal(file.getSize())
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


# saved_files = pairs of (file.id,disk.id)\ **not sure if this the way to sub the size**
# SUB files.size FROM disks.free_space WHERE disk_id = dId


def addFileToDisk(file: File, diskID: int) -> Status:
    # Insert into saved_files.(FK violation , UNIQUE violation)
    # update free space. (CHECK violation)
    # catch errors with rollback.
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
                    INSERT INTO saved_files(file_id,disk_id)  
                    VALUES({fId},{dId}); 
                    UPDATE disks SET free_space=(free_space-{size}) WHERE disk_id={dId}; 
            """
        ).format(
            fId=sql.Literal(file.getFileID()),
            dId=sql.Literal(diskID),
            size=sql.Literal(file.getSize())
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    # remove from saved_files
    # update free space
    # catch errors
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM saved_files where file_id={fID} AND disk_id={dID}; "
            "UPDATE disks SET free_space=(free_space+{size}) WHERE disk_id={dID}; "
            "DELETE FROM saved_files WHERE file_id={fID} AND disk_id={dID}; "
        ).format(
            fID=sql.Literal(file.getFileID()),
            dID=sql.Literal(diskID),
            size=sql.Literal(file.getSize())
        )
        rows_effected, rows = conn.execute(query)
        if rows_effected < 1:
            conn.rollback()
        else:
            conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return Status.ERROR
    conn.close()
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    # Insert into disks_ram_enhanced
    # catch errors
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
                    INSERT INTO disks_ram_enhanced(ram_id,disk_id)  
                    VALUES({rID},{dID}) 
            """
        ).format(
            rID=sql.Literal(ramID),
            dID=sql.Literal(diskID)
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except Exception as e:
        return Status.ERROR
    finally:
        conn.close()
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    # remove from disks_ram_enhanced
    # catch errors
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM disks_ram_enhanced WHERE ram_id={rID} AND disk_id={dID} "
        ).format(
            rID=sql.Literal(ramID),
            dID=sql.Literal(diskID)
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    # use the view saved_files_file_details
    # where this diskID.
    # and aggregate (AVG) (if empty will return NULL - we should convert to zero)
    # catch errors
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT COALESCE(AVG(size),0) as size_avg 
            FROM saved_files_file_details 
            WHERE disk_id={dID}
            """
        ).format(
            dID=sql.Literal(diskID)
        )
        rows_affected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return result[0]["size_avg"]


def totalRAMonDisk(diskID: int) -> int:
    # use the view disks_ram_enhanced_ram_details
    # where this diskID.
    # and aggregate (SUM) (if empty will return NULL - we should convert to zero)
    # catch errors
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT COALESCE(SUM(size),0) AS size_sum 
            FROM disks_ram_enhanced_ram_details  
            WHERE disk_id={dID}"""
        ).format(
            dID=sql.Literal(diskID)
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return result[0]["size_sum"]


def getCostForType(type: str) -> int:
    # join view saved_files_file_details where this Type
    # with vew saved_files_disk_details where this Type
    # ON pair key (file_id,disk_id)
    # where Type
    # then agg (SUM)(if empty will return NULL - we should convert to zero)
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT COALESCE(SUM(fDetails.size * dDetails.cost_per_byte),0) as total_cost "
            "FROM (SELECT * FROM saved_files_file_details WHERE type = {file_type}) fDetails "
            "INNER JOIN saved_files_disk_details dDetails "
            "ON fDetails.file_id = dDetails.file_id "
            "AND fDetails.disk_id = dDetails.disk_id "
        ).format(
            file_type=sql.Literal(type)
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return result[0]["total_cost"]


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    # FILES ORDERED By SIZE THEN BY ID'S
    # where size <= freespace of this disk (sub-query)
    # LIMIT 5
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT file_id 
            FROM files 
            WHERE size<=(SELECT free_space FROM disks WHERE disk_id={dID})
            ORDER BY file_id DESC
            LIMIT 5 """
        ).format(
            dID=sql.Literal(diskID)
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    # need to convert (check the output) (maybe should concatenate result.rows)
    # something like return [next(iter(row)) for row in result.rows]
    return [next(iter(row)) for row in result.rows]


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    # check if we can use "diskTotalRAM" func.
    # if not, then
    # FILES ORDERED By SIZE THEN BY ID'S
    # where size <= freespace of this disk (sub-query)
    # where size <= sum of... (same sub-query used in the func "diskTotalRAM")
    # LIMIT 5
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT file_id "
            "FROM files "
            "WHERE size <= (SELECT free_space FROM disks WHERE disk_id={dID}) "
            "AND size <= ( "
            "SELECT COALESCE(SUM(size),0) as size_sum "
            "FROM disks_ram_enhanced_ram_details "
            "WHERE disk_id={dID} "
            ") "
            "ORDER BY file_id ASC "
            "LIMIT 5 "
        ).format(
            dID=sql.Literal(diskID)
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [next(iter(row)) for row in result.rows]


def isCompanyExclusive(diskID: int) -> bool:
    # join the Ram VIEWS (SAME AS FILES VIEWS)
    #  where this disk_id
    # SELECT * ... where manufacturing_company != company
    # if the length of the result is zero then true.
    # WARNING : check the empty case
    # means when no rams associated with the disk.(should return true)
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            (SELECT disk_id 
            FROM rams_And_Disks_Details 
            WHERE disk_id={dID} 
            AND ram_company != disk_company) UNION (
                /* trick to check if disk EXISTS*/
                SELECT * FROM(VALUES(1)) a
                WHERE NOT EXISTS (
                    SELECT * FROM disks WHERE disk_id={dID}  
                )
            ) """
        ).format(
            dID=sql.Literal(diskID)
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return False
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return result.isEmpty()


def getConflictingDisks() -> List[int]:
    # SELECT DISTINCT a1.disk_id
    # FROM saved_files a1
    # INNER join saved_files a2
    # on a1.file_id = a2.file_id
    # WHERE a1.disk_id != a2.disk_id
    # ORDER by a1.disk_id ASC
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT DISTINCT leftCopy.disk_id 
            FROM saved_files leftCopy 
            INNER join saved_files rightCopy 
            ON leftCopy.file_id=rightCopy.file_id 
            WHERE leftCopy.disk_id!=rightCopy.disk_id 
            ORDER by leftCopy.disk_id ASC"""
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [next(iter(row)) for row in result.rows]


def mostAvailableDisks() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT disks.disk_id,disks.speed,file_counts.files_count FROM 
            disks INNER JOIN (
                SELECT disk_id, COUNT(*) AS files_count 
                FROM ( 
                    SELECT disks.disk_id,files.file_id FROM  
                    disks LEFT JOIN files  
                    ON disks.free_space >= COALESCE(files.size,0) 
                ) available
            GROUP BY disk_id 
            ) file_counts 
            ON disks.disk_id = file_counts.disk_id 
            ORDER BY file_counts.files_count DESC, disks.speed DESC, disks.disk_id ASC 
            LIMIT 5 
            """
        )
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [next(iter(row)) for row in result.rows]


def getCloseFiles(fileID: int) -> List[int]:
    # files where this file id
    # SELECT disk_id from saved_files where this file_id = relevant_disk_ids

    # join saved_files with relevant_disk_ids on disk_id
    # now we have table with files saved on the relevant disks.
    # group by file_id aggregate COUNT disk_id. = call it relevant_disks_count
    # join relevant_disks_count with files on file id, project only file_id, countOFDisks.
    # convert NULLs to zeroes
    # order by ID
    # get top 10 with the condition >= x/2
    # when x is the result of sub-query that returns the disks count of this file_id.
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT file_id FROM (
            SELECT other_files.file_id,COALESCE(relevant_disks_count.count_of_disks,0) as count_of_disks 
            FROM ( 
                    SELECT file_id FROM files  
                    WHERE file_id != {fID} 
                ) other_files LEFT JOIN ( 
                SELECT saved_files.file_id, COUNT(*) AS count_of_disks 
                FROM saved_files INNER JOIN (
                    SELECT disk_id 
                    FROM saved_files 
                    WHERE file_id = {fID} 
                ) relevant_disk_ids 
                ON saved_files.disk_id = relevant_disk_ids.disk_id 
                GROUP BY saved_files.file_id 
            ) relevant_disks_count 
            ON other_files.file_id = relevant_disks_count.file_id 
            ) files_count_data 
            WHERE 2 * count_of_disks >= (SELECT COUNT(*) FROM saved_files WHERE file_id={fID}) 
            ORDER BY count_of_disks,file_id
            LIMIT 10
            """
        ).format(
            fID=sql.Literal(fileID)
        )
        _, result = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [next(iter(row)) for row in result.rows]

#include <stdio.h>
#include <string.h>
#include "pds.h"

// Structure to hold repository information
struct PDS_RepoInfo repo_handle = {.pds_data_fp = NULL, .repo_status = PDS_REPO_CLOSED, .rec_size = 0};

// Function to create a new repository
int pds_create(char *repo_name) {
    char filename[26];
    sprintf(filename, "%s.dat", repo_name);
    //printf("The name of the file to be created %s", repo_name);
    FILE *fp = fopen(filename, "wb");
    fclose(fp);
    return PDS_SUCCESS;
}

// Function to open an existing repository
int pds_open(char *repo_name, int rec_size) {
    char filename[26];
    sprintf(filename, "%s.dat", repo_name);

    if (repo_handle.repo_status == PDS_REPO_OPEN) {
        // returns successful status whenever a file is already open
        return PDS_SUCCESS;
    }
    else {
        repo_handle.pds_data_fp = fopen(filename, "rb+");
        if (repo_handle.pds_data_fp == NULL) {
            // returns failure status whenever a file is not found
            return PDS_FILE_ERROR;
        }
        else {
            strcpy(repo_handle.pds_name, repo_name);
            repo_handle.rec_size = rec_size;
            repo_handle.repo_status = PDS_REPO_OPEN;
            // returns successful status whenever an existing file is successfully opened
            return PDS_SUCCESS;
        }
    }
}

// Function to add a record to the repository using a key
int put_rec_by_key(int key, void *rec) {
    if (repo_handle.repo_status == PDS_REPO_CLOSED) {
        // returns failure status whenever the file is not open
        return PDS_FILE_ERROR;
    }
    else {
        fseek(repo_handle.pds_data_fp, 0, SEEK_END);
        fwrite(&key, sizeof(int), 1, repo_handle.pds_data_fp);
        fwrite(rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);
        // returns successful status whenever a record is successfully added
        return PDS_SUCCESS;
    }
}

// Function to retrieve a record from the repository using a key
int get_rec_by_key(int key, void *rec) {
    if (repo_handle.repo_status == PDS_REPO_CLOSED) {
        // Return failure status if the repository is closed
        return PDS_FILE_ERROR;
    }

    int k;
    fseek(repo_handle.pds_data_fp, 0, SEEK_SET);

    while (1) {
        // Read key
        if (fread(&k, sizeof(int), 1, repo_handle.pds_data_fp) != 1) {
            if (feof(repo_handle.pds_data_fp)) {
                break;  // End of file
            } else {
                return PDS_FILE_ERROR;  // Read error
            }
        }

        // Read record
        if (fread(rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp) != 1) {
            if (feof(repo_handle.pds_data_fp)) {
                break;  // End of file
            } else {
                return PDS_FILE_ERROR;  // Read error
            }
        }

        // Check if key matches
        if (k == key) {
            return PDS_SUCCESS;  // Record found
        }
    }

    return PDS_REC_NOT_FOUND;  // Record not found
}


// Function to close the repository
int pds_close() {
    if (repo_handle.repo_status == PDS_REPO_CLOSED) {
        // returns successful status whenever a file is already closed
        return PDS_SUCCESS;
    }
    else {
        fclose(repo_handle.pds_data_fp);
        repo_handle.pds_data_fp = NULL;
        repo_handle.repo_status = PDS_REPO_CLOSED;
        // returns successful status whenever a file is successfully closed
        return PDS_SUCCESS;
    }
}
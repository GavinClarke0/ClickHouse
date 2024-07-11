#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/InMemoryPathMap.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <filesystem>
#include <map>

namespace DB
{

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    std::string key_prefix;
    InMemoryPathMap & path_map;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    bool write_created = false;
    bool write_finalized = false;

public:
    // Assuming that paths are normalized.
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        std::filesystem::path && path_,
        std::string && key_prefix_,
        InMemoryPathMap & path_map_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};

class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path_from;
    std::filesystem::path path_to;
    InMemoryPathMap & path_map;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    bool write_created = false;
    bool write_finalized = false;

    std::unique_ptr<WriteBufferFromFileBase>
    createWriteBuf(const std::filesystem::path & expected_path, const std::filesystem::path & new_path, bool validate_content);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        std::filesystem::path && path_from_,
        std::filesystem::path && path_to_,
        InMemoryPathMap & path_map_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};

class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;

    InMemoryPathMap & path_map;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    std::string key_prefix;
    bool removed = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        std::filesystem::path && path_,
        InMemoryPathMap & path_map_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};

}

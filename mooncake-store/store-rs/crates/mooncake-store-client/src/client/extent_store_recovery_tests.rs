    #[test]
    fn extent_store_recovers_materialized_locator_after_restart() {
        let root = test_extent_store_root("restart");
        let payload = b"recover-me".to_vec();
        let locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 2)
                .expect("extent store should start");
            engine
                .put("logical/restart", &payload)
                .expect("extent store write should succeed")
        };

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 2)
            .expect("extent store should restart");
        let recovered = engine
            .get(&locator, payload.len() as u64)
            .expect("extent store read should succeed")
            .expect("extent store object should exist");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn extent_store_recovers_record_larger_than_segment_size() {
        let root = test_extent_store_root("expanded-segment-restart");
        let segment_size = EXTENT_STORE_ALIGNMENT * 2;
        let payload = vec![9u8; segment_size as usize];
        let locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should start");
            engine
                .put("logical/expanded-segment", &payload)
                .expect("large extent store write should succeed")
        };
        let decoded = ExtentStoreLocator::decode(&locator).expect("locator should decode");
        assert!(decoded.record_len > segment_size);
        let segment_path = root
            .path()
            .join("segments")
            .join(format!("{:016x}.seg", decoded.segment_id));
        assert!(
            std::fs::metadata(&segment_path)
                .expect("expanded segment file should exist")
                .len()
                > segment_size
        );

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should recover expanded segment");
        let recovered = engine
            .get(&locator, payload.len() as u64)
            .expect("expanded extent store read should succeed")
            .expect("expanded extent store object should exist");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn extent_store_recovers_legacy_preallocated_segment_tail() {
        let root = test_extent_store_root("legacy-preallocated-restart");
        let segment_size = EXTENT_STORE_ALIGNMENT * 16;
        let payload = b"legacy-preallocated".to_vec();
        let locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should start");
            engine
                .put("logical/legacy-preallocated", &payload)
                .expect("extent store write should succeed")
        };
        let decoded = ExtentStoreLocator::decode(&locator).expect("locator should decode");
        let segment_path = root
            .path()
            .join("segments")
            .join(format!("{:016x}.seg", decoded.segment_id));
        std::fs::OpenOptions::new()
            .write(true)
            .open(&segment_path)
            .expect("segment should open for legacy sizing")
            .set_len(segment_size)
            .expect("segment should mimic legacy full preallocation");

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should recover legacy preallocated segment");
        let recovered = engine
            .get(&locator, payload.len() as u64)
            .expect("legacy preallocated extent store read should succeed")
            .expect("legacy preallocated extent store object should exist");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn extent_store_seals_legacy_unaligned_tail_before_new_writes() {
        let root = test_extent_store_root("legacy-unaligned-tail");
        let segment_size = EXTENT_STORE_ALIGNMENT * 4;
        let logical_locator = "legacy/dense";
        let payload = vec![0x6du8; EXTENT_STORE_ALIGNMENT as usize];
        let value_offset = EXTENT_STORE_HEADER_LEN as u64 + logical_locator.len() as u64;
        let record_len = value_offset + payload.len() as u64;
        assert!(!is_aligned_u64(value_offset));
        assert!(!is_aligned_u64(record_len));

        let segments = root.path().join("segments");
        std::fs::create_dir_all(&segments).expect("segments directory should be created");
        let path = segments.join(format!("{:016x}.seg", 1));
        let mut record = vec![0u8; record_len as usize];
        encode_record_header(
            &mut record[..EXTENT_STORE_HEADER_LEN],
            EXTENT_STORE_RECORD_KIND_SINGLE,
            logical_locator.len() as u64,
            payload.len() as u64,
            payload_checksum(&payload),
            value_offset,
            record_len,
        );
        record[EXTENT_STORE_HEADER_LEN..EXTENT_STORE_HEADER_LEN + logical_locator.len()]
            .copy_from_slice(logical_locator.as_bytes());
        record[value_offset as usize..].copy_from_slice(&payload);
        std::fs::write(&path, &record).expect("legacy dense record should be written");

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("legacy dense segment should recover");
        let legacy_locator = ExtentStoreLocator {
            segment_id: 1,
            offset: 0,
            record_len,
            value_offset,
            value_len: payload.len() as u64,
            generation: 1,
        }
        .encode();
        assert_eq!(
            engine
                .get(&legacy_locator, payload.len() as u64)
                .expect("legacy payload should read")
                .expect("legacy payload should exist"),
            payload
        );

        let next = engine
            .put("aligned/after-upgrade", b"new-data")
            .expect("new aligned record should write");
        let next = ExtentStoreLocator::decode(&next).expect("new locator should decode");
        assert_eq!(next.segment_id, 2);
        assert_eq!(next.offset, 0);
        assert!(is_aligned_u64(next.value_offset));
        assert!(is_aligned_u64(next.record_len));
    }

    #[test]
    fn legacy_dense_aligned_value_start_without_padding_uses_buffered_read() {
        let root = test_extent_store_root("legacy-aligned-start-unpadded-tail");
        let segment_size = EXTENT_STORE_ALIGNMENT * 4;
        let logical_locator = "k".repeat(EXTENT_STORE_ALIGNMENT as usize - EXTENT_STORE_HEADER_LEN);
        let payload = vec![0x7du8; EXTENT_STORE_ALIGNMENT as usize + 1];
        let value_offset = EXTENT_STORE_HEADER_LEN as u64 + logical_locator.len() as u64;
        let record_len = value_offset + payload.len() as u64;
        assert!(is_aligned_u64(value_offset));
        assert!(!is_aligned_u64(record_len));

        let segments = root.path().join("segments");
        std::fs::create_dir_all(&segments).expect("segments directory should be created");
        let path = segments.join(format!("{:016x}.seg", 1));
        let mut record = vec![0u8; record_len as usize];
        encode_record_header(
            &mut record[..EXTENT_STORE_HEADER_LEN],
            EXTENT_STORE_RECORD_KIND_SINGLE,
            logical_locator.len() as u64,
            payload.len() as u64,
            payload_checksum(&payload),
            value_offset,
            record_len,
        );
        record[EXTENT_STORE_HEADER_LEN..EXTENT_STORE_HEADER_LEN + logical_locator.len()]
            .copy_from_slice(logical_locator.as_bytes());
        record[value_offset as usize..].copy_from_slice(&payload);
        std::fs::write(&path, &record).expect("legacy dense record should be written");

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("legacy dense segment should recover");
        let legacy_locator = ExtentStoreLocator {
            segment_id: 1,
            offset: 0,
            record_len,
            value_offset,
            value_len: payload.len() as u64,
            generation: 1,
        };
        assert_eq!(direct_scratch_read_len(&legacy_locator), None);
        let encoded = legacy_locator.encode();
        let before = engine.io_stats();
        let mut destination = vec![0u8; payload.len()];
        assert_eq!(
            engine
                .get_into(&encoded, payload.len() as u64, &mut destination)
                .expect("legacy unpadded payload should read"),
            Some(payload.len())
        );
        let delta = engine.io_stats().delta_since(before);
        assert_eq!(destination, payload);
        assert_eq!(delta.direct_scratch_read_ops, 0);
    }

    #[test]
    fn legacy_deleted_unaligned_tail_is_removed_during_recovery() {
        let root = test_extent_store_root("legacy-deleted-tail-cleanup");
        let segment_size = EXTENT_STORE_ALIGNMENT * 4;
        let logical_locator = "legacy/deleted-tail";
        let payload = vec![0x55u8; EXTENT_STORE_ALIGNMENT as usize + 17];
        let value_offset = EXTENT_STORE_HEADER_LEN as u64 + logical_locator.len() as u64;
        let record_len = value_offset + payload.len() as u64;
        assert!(!is_aligned_u64(record_len));

        let segments = root.path().join("segments");
        let meta = root.path().join("meta");
        std::fs::create_dir_all(&segments).expect("segments directory should be created");
        std::fs::create_dir_all(&meta).expect("meta directory should be created");
        let segment_path = segments.join(format!("{:016x}.seg", 1));
        let mut record = vec![0u8; record_len as usize];
        encode_record_header(
            &mut record[..EXTENT_STORE_HEADER_LEN],
            EXTENT_STORE_RECORD_KIND_SINGLE,
            logical_locator.len() as u64,
            payload.len() as u64,
            payload_checksum(&payload),
            value_offset,
            record_len,
        );
        record[EXTENT_STORE_HEADER_LEN..EXTENT_STORE_HEADER_LEN + logical_locator.len()]
            .copy_from_slice(logical_locator.as_bytes());
        record[value_offset as usize..].copy_from_slice(&payload);
        std::fs::write(&segment_path, &record).expect("legacy dense record should be written");

        let locator = ExtentStoreLocator {
            segment_id: 1,
            offset: 0,
            record_len,
            value_offset,
            value_len: payload.len() as u64,
            generation: 1,
        };
        std::fs::write(
            delete_journal_path(root.path()),
            encode_delete_journal_record(&locator),
        )
        .expect("legacy delete journal should be written");

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("legacy deleted segment should recover");
        assert!(!segment_path.exists());
        assert_eq!(engine.inner.lock().active_segment_id, 2);
        let next = engine
            .put("aligned/after-deleted-legacy", b"new-data")
            .expect("new aligned record should write");
        let next = ExtentStoreLocator::decode(&next).expect("new locator should decode");
        assert_eq!(next.segment_id, 2);
        assert_eq!(next.offset, 0);
    }

    #[test]
    fn extent_store_persists_delete_across_restart() {
        let root = test_extent_store_root("delete-restart");
        let payload = b"delete-me".to_vec();
        let locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 3)
                .expect("extent store should start");
            engine
                .put("logical/delete-restart", &payload)
                .expect("extent store write should succeed")
        };
        {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 3)
                .expect("extent store should restart before delete");
            assert!(engine.delete(&locator).expect("delete should succeed"));
            assert!(!engine.delete(&locator).expect("second delete should be idempotent"));
        }

        let journal_path = delete_journal_path(root.path());
        assert_eq!(
            std::fs::metadata(&journal_path)
                .expect("delete journal should exist after delete")
                .len(),
            EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN as u64
        );

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 3)
            .expect("extent store should restart after delete");
        {
            let inner = engine.inner.lock();
            let decoded = ExtentStoreLocator::decode(&locator).expect("locator should decode");
            let segment = inner
                .segments
                .get(&decoded.segment_id)
                .expect("active segment should remain open");
            assert!(segment
                .deleted_extents
                .contains(&deleted_record_extent(&decoded)));
        }
        assert_eq!(
            std::fs::metadata(&journal_path)
                .expect("delete journal should survive recovery for active delete")
                .len(),
            EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN as u64
        );
        assert!(!engine.delete(&locator).expect("recovered delete should remain idempotent"));
    }

    #[test]
    fn extent_store_recovery_removes_fully_dead_sealed_segment() {
        let root = test_extent_store_root("delete-sealed-restart");
        let segment_size = EXTENT_STORE_ALIGNMENT * 2;
        let payload = vec![7u8; EXTENT_STORE_ALIGNMENT as usize];
        let (first_locator, second_locator) = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should start");
            let first = engine
                .put("logical/sealed-first", &payload)
                .expect("first write should succeed");
            let second = engine
                .put("logical/sealed-second", &payload)
                .expect("second write should succeed");
            (first, second)
        };
        let first_segment = ExtentStoreLocator::decode(&first_locator)
            .expect("first locator should decode")
            .segment_id;
        let second_segment = ExtentStoreLocator::decode(&second_locator)
            .expect("second locator should decode")
            .segment_id;
        assert!(second_segment > first_segment);
        let first_segment_path = root
            .path()
            .join("segments")
            .join(format!("{first_segment:016x}.seg"));

        {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should restart before delete");
            assert!(engine.delete(&first_locator).expect("delete should succeed"));
        }

        let journal_path = delete_journal_path(root.path());
        assert_eq!(
            std::fs::metadata(&journal_path)
                .expect("delete journal should exist before recovery compaction")
                .len(),
            EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN as u64
        );

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should restart after delete");
        assert!(!first_segment_path.exists());
        assert_eq!(
            std::fs::metadata(&journal_path)
                .expect("delete journal should remain after compaction")
                .len(),
            0
        );
        assert!(engine
            .get(&second_locator, payload.len() as u64)
            .expect("second locator read should succeed")
            .is_some());
    }

    #[test]
    fn extent_store_recovery_removes_stale_empty_segment_file() {
        let root = test_extent_store_root("empty-segment-restart");
        let segment_size = EXTENT_STORE_ALIGNMENT * 4;
        let payload = b"keep-live-data".to_vec();
        let locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should start");
            engine
                .put("logical/live", &payload)
                .expect("live write should succeed")
        };
        let stale_segment_path = root.path().join("segments").join(format!("{:016x}.seg", 2));
        let stale_segment = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&stale_segment_path)
            .expect("stale empty segment should open");
        stale_segment
            .set_len(segment_size)
            .expect("stale empty segment should be preallocated");
        assert!(stale_segment_path.exists());

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should restart with stale empty segment");
        assert!(!stale_segment_path.exists());
        let recovered = engine
            .get(&locator, payload.len() as u64)
            .expect("live locator should still read after cleanup")
            .expect("live extent should still exist");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn extent_store_restart_rejects_corrupt_committed_record() {
        let root = test_extent_store_root("corrupt-restart");
        let payload = b"corrupt-me".to_vec();
        let locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(
                root.path(),
                EXTENT_STORE_ALIGNMENT * 2,
            )
            .expect("extent store should start");
            engine
                .put("logical/corrupt", &payload)
                .expect("extent store write should succeed")
        };
        let decoded = ExtentStoreLocator::decode(&locator).expect("locator should decode");
        let path = root
            .path()
            .join("segments")
            .join(format!("{:016x}.seg", decoded.segment_id));
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("segment should open for corruption");
        file.write_at(
            &[payload[0] ^ 0xFF],
            decoded.offset + decoded.value_offset,
        )
        .expect("segment payload corruption should succeed");

        let error = match ExtentStoreEngine::new_with_segment_size(
            root.path(),
            EXTENT_STORE_ALIGNMENT * 2,
        ) {
            Ok(_) => panic!("extent store restart should reject corrupt committed payload"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::InvalidState(_)));
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn extent_store_multiple_deletes_compact_journal_on_recovery() {
        let root = test_extent_store_root("multi-delete-journal");
        let segment_size = EXTENT_STORE_ALIGNMENT * 8;
        let payload = vec![5u8; 128];
        let (loc1, loc2, loc3) = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should start");
            let a = engine.put("multi/a", &payload).expect("put a");
            let b = engine.put("multi/b", &payload).expect("put b");
            let c = engine.put("multi/c", &payload).expect("put c");
            (a, b, c)
        };
        {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store restart");
            assert!(engine.delete(&loc1).expect("delete a"));
            assert!(engine.delete(&loc2).expect("delete b"));
        }
        let journal_path = delete_journal_path(root.path());
        let journal_size = std::fs::metadata(&journal_path)
            .expect("journal should exist")
            .len();
        assert_eq!(journal_size, 2 * EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN as u64);

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store restart after multi-delete");
        // Deleted records should remain absent
        assert!(engine.get(&loc1, payload.len() as u64).is_err());
        assert!(engine.get(&loc2, payload.len() as u64).is_err());
        // Surviving record should remain readable
        let data = engine
            .get(&loc3, payload.len() as u64)
            .expect("get c should succeed")
            .expect("c should exist");
        assert_eq!(data, payload);
    }

    #[test]
    fn extent_store_fresh_start_creates_empty_engine() {
        let root = test_extent_store_root("fresh-start");
        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 4)
            .expect("fresh extent store should start");
        let stats = engine.maintenance_stats();
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.dead_bytes, 0);
        assert_eq!(stats.segment_count, 1); // active segment is always present
    }

    #[test]
    fn extent_store_recovers_active_segment_after_full_tail() {
        let root = test_extent_store_root("full-tail");
        let segment_size = EXTENT_STORE_ALIGNMENT * 2;
        let payload = vec![7u8; EXTENT_STORE_ALIGNMENT as usize];
        let first_locator = {
            let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
                .expect("extent store should start");
            engine
                .put("logical/first", &payload)
                .expect("first extent store write should succeed")
        };

        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should restart");
        let second_locator = engine
            .put("logical/second", &payload)
            .expect("second extent store write should succeed");
        let first_segment = ExtentStoreLocator::decode(&first_locator)
            .expect("first locator should decode")
            .segment_id;
        let second_segment = ExtentStoreLocator::decode(&second_locator)
            .expect("second locator should decode")
            .segment_id;
        assert!(second_segment > first_segment);
    }

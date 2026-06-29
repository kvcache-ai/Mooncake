    #[test]
    fn extent_store_unregisters_fixed_files_before_segment_unlink() {
        let root = test_extent_store_root("fixed-file-unlink");
        let segment_size = EXTENT_STORE_ALIGNMENT * 2;
        let payload = vec![9u8; EXTENT_STORE_ALIGNMENT as usize];
        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should start");
        let first = engine
            .put("logical/fixed-file-first", &payload)
            .expect("first write should succeed");
        let second = engine
            .put("logical/fixed-file-second", &payload)
            .expect("second write should succeed");
        let first_segment = ExtentStoreLocator::decode(&first)
            .expect("first locator should decode")
            .segment_id;
        let second_segment = ExtentStoreLocator::decode(&second)
            .expect("second locator should decode")
            .segment_id;
        assert!(second_segment > first_segment);
        let first_segment_path = root
            .path()
            .join("segments")
            .join(format!("{first_segment:016x}.seg"));
        let mut direct_dst = AlignedExtentStoreBuffer::zeroed(payload.len())
            .expect("aligned destination should allocate");
        assert_eq!(
            engine
                .get_into(&first, payload.len() as u64, &mut direct_dst)
                .expect("direct-eligible read should succeed before delete"),
            Some(payload.len())
        );
        assert_eq!(&direct_dst[..payload.len()], payload.as_slice());
        let mut buffered_dst = vec![0u8; payload.len()];
        assert_eq!(
            engine
                .get_into(&first, payload.len() as u64, &mut buffered_dst)
                .expect("buffered read should succeed before delete"),
            Some(payload.len())
        );
        assert_eq!(buffered_dst, payload);
        assert!(engine.delete(&first).expect("delete should succeed"));
        assert!(!first_segment_path.exists());
        let after_delete = engine.io_stats();
        assert_eq!(after_delete.fixed_file_slot_exhaustions, 0);
        let mut second_dst = vec![0u8; payload.len()];
        assert_eq!(
            engine
                .get_into(&second, payload.len() as u64, &mut second_dst)
                .expect("surviving segment read should succeed"),
            Some(payload.len())
        );
        assert_eq!(second_dst, payload);
    }

    #[test]
    fn fixed_file_registry_keeps_buffered_and_direct_slots_distinct() {
        let counters = std::sync::Arc::new(ExtentStoreIoCounters::default());
        let mut ring = match IoUring::new(8) {
            Ok(ring) => ring,
            Err(error) if io_uring_unavailable(&error) => return,
            Err(error) => panic!("io_uring should initialize: {error}"),
        };
        let mut registry = match ExtentStoreFixedFileRegistry::new(&mut ring, counters) {
            Ok(registry) => registry,
            Err(_) => return,
        };
        let root = test_extent_store_root("fixed-file-key-distinct");
        let path = root.path().join("segment.tmp");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("segment file should open");

        let buffered_slot = registry
            .fixed(
                &mut ring,
                ExtentStoreFixedFileKey::for_buffered(1),
                file.as_raw_fd(),
            )
            .expect("buffered fixed slot should register")
            .0;
        let direct_slot = registry
            .fixed(
                &mut ring,
                ExtentStoreFixedFileKey::for_direct(1),
                file.as_raw_fd(),
            )
            .expect("direct fixed slot should register")
            .0;
        assert_ne!(direct_slot, buffered_slot);
    }

    #[test]
    fn extent_store_get_batch_into_reads_multiple_locators() {
        let root = test_extent_store_root("batch-into");
        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 64)
            .expect("engine should start");
        let payloads: Vec<Vec<u8>> = (0..4)
            .map(|i| vec![i as u8; 64 + i * 16])
            .collect();
        let locators: Vec<String> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| engine.put(&format!("batch-read/{i}"), p).expect("put should succeed"))
            .collect();
        let mut buffers: Vec<Vec<u8>> = payloads.iter().map(|p| vec![0u8; p.len()]).collect();
        let mut reads: Vec<ExtentStoreRead<'_>> = locators
            .iter()
            .zip(buffers.iter_mut())
            .zip(payloads.iter())
            .map(|((loc, buf), p)| ExtentStoreRead {
                locator: loc.as_str(),
                expected_len: p.len() as u64,
                dst: buf.as_mut_slice(),
            })
            .collect();
        let results = engine.get_batch_into(&mut reads);
        for (i, result) in results.iter().enumerate() {
            let bytes = result
                .as_ref()
                .expect("batch read should succeed")
                .expect("data should exist");
            assert_eq!(bytes, payloads[i].len());
            assert_eq!(&buffers[i][..bytes], payloads[i].as_slice());
        }
    }

    #[test]
    fn extent_store_io_stats_tracks_read_and_write_ops() {
        let root = test_extent_store_root("io-stats-tracking");
        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 128)
            .expect("engine should start");
        let stats_before = engine.io_stats();
        // Use a payload larger than packed-block threshold so it goes through direct/buffered write path.
        let payload = vec![42u8; EXTENT_STORE_ALIGNMENT as usize];
        let locator = engine.put("io/stats", &payload).expect("put should succeed");
        let stats_after_write = engine.io_stats();
        // Check that some write counter increased (direct, scratch, or buffered)
        assert!(
            stats_after_write.direct_write_ops > stats_before.direct_write_ops
                || stats_after_write.direct_scratch_write_ops > stats_before.direct_scratch_write_ops
                || stats_after_write.buffered_write_ops > stats_before.buffered_write_ops
        );

        let mut buf = vec![0u8; payload.len()];
        engine.get_into(&locator, payload.len() as u64, &mut buf).expect("get should succeed");
        let stats_after_read = engine.io_stats();
        // Check that some read counter increased (direct, scratch, or buffered)
        assert!(
            stats_after_read.direct_read_ops > stats_after_write.direct_read_ops
                || stats_after_read.direct_scratch_read_ops > stats_after_write.direct_scratch_read_ops
                || stats_after_read.buffered_read_ops > stats_after_write.buffered_read_ops
        );
    }

    #[test]
    fn fixed_file_registry_reuses_slot_after_unregister() {
        let counters = std::sync::Arc::new(ExtentStoreIoCounters::default());
        let mut ring = match IoUring::new(8) {
            Ok(ring) => ring,
            Err(error) if io_uring_unavailable(&error) => return,
            Err(error) => panic!("io_uring should initialize: {error}"),
        };
        let mut registry = match ExtentStoreFixedFileRegistry::new(&mut ring, counters) {
            Ok(registry) => registry,
            Err(_) => return,
        };
        let root = test_extent_store_root("fixed-file-registry");
        let first_path = root.path().join("first.tmp");
        let second_path = root.path().join("second.tmp");
        let first = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&first_path)
            .expect("first file should open");
        let second = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&second_path)
            .expect("second file should open");

        let slot = registry
            .fixed(
                &mut ring,
                ExtentStoreFixedFileKey::for_buffered(1),
                first.as_raw_fd(),
            )
            .expect("first fixed slot should register")
            .0;
        registry
            .unregister_key(&mut ring, ExtentStoreFixedFileKey::for_buffered(1))
            .expect("fixed slot should unregister");
        let reused = registry
            .fixed(
                &mut ring,
                ExtentStoreFixedFileKey::for_buffered(2),
                second.as_raw_fd(),
            )
            .expect("second fixed slot should register")
            .0;
        assert_eq!(reused, slot);
    }

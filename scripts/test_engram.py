#!/usr/bin/env python3
"""
Comprehensive test suite for C++ Engram implementation.

Tests cover:
1. Configuration classes (EngramConfig, BackboneConfig)
2. Engram creation and store population
3. Forward pass with various batch/sequence sizes
4. Edge cases (single token, padding, large vocab IDs)
5. Memory layout correctness
6. Numerical stability
7. Error handling

Usage:
    # Start mooncake master first:
    # build/mooncake-store/src/mooncake_master --enable_http_metadata_server=true --eviction_high_watermark_ratio=0.95
    
    # Set environment:
    export MOONCAKE_MASTER=127.0.0.1:50051
    export MOONCAKE_TE_META_DATA_SERVER=http://127.0.0.1:8080/metadata
    
    # Run test:
    source /root/sglang-venv/bin/activate
    python scripts/test_engram.py
"""

import os
import sys
import numpy as np
import unittest

# Import mooncake Python modules
from mooncake.mooncake_config import MooncakeConfig

# Global variables
GLOBAL_STORE = None
STORE_MODULE = None  # store.so module from build/mooncake-integration

def import_store_module():
    """Import store.so module from build/mooncake-integration."""
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    build_store = os.path.join(repo_root, "build", "mooncake-integration")
    
    if not os.path.isdir(build_store):
        raise ImportError(f"build/mooncake-integration not found. Build Mooncake with Engram: cd build && cmake .. -DWITH_STORE=ON && make -j 128")
    
    was_in_path = build_store in sys.path
    if not was_in_path:
        sys.path.insert(0, build_store)
    
    try:
        import importlib
        store_module = importlib.import_module('store')
        print("✅ store.so imported successfully from build/mooncake-integration")
        return store_module
    except Exception as e:
        print(f"❌ Failed to import store.so: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_engram(store_module):
    """Extract Engram bindings from store module."""
    Engram = store_module.Engram
    EngramConfig = store_module.EngramConfig
    BackboneConfig = store_module.BackboneConfig
    return Engram, EngramConfig, BackboneConfig

def create_store_connection(store_module):
    """Create and connect to the Store."""
    MooncakeDistributedStore = store_module.MooncakeDistributedStore
    store = MooncakeDistributedStore()
    config = MooncakeConfig.load_from_env()
    print(f"[{os.getpid()}] Connecting to Mooncake Master at {config.master_server_address}...")
    
    rc = store.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name,
        config.master_server_address,
    )
    
    if rc != 0:
        raise RuntimeError(f"Failed to setup mooncake store, error code: {rc}")
    
    print("✅ Store connection established")
    return store, config

def setUpModule():
    """Executed once before all tests."""
    global GLOBAL_STORE, STORE_MODULE
    try:
        STORE_MODULE = import_store_module()
        GLOBAL_STORE, _ = create_store_connection(STORE_MODULE)
    except Exception as e:
        print(f"❌ Failed to establish global store connection: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def tearDownModule():
    """Executed once after all tests."""
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        GLOBAL_STORE.close()
        GLOBAL_STORE = None

class EngramTestBase(unittest.TestCase):
    """Base test class for Engram tests."""
    
    def setUp(self):
        """Executed before each test method."""
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")
        
        self.store = GLOBAL_STORE
        Engram, EngramConfig, BackboneConfig = import_engram(STORE_MODULE)
        self.Engram = Engram
        self.EngramConfig = EngramConfig
        self.BackboneConfig = BackboneConfig
        
        # Clean store before each test
        self.store.remove_all()
    
    def create_default_configs(self):
        """Create default EngramConfig and BackboneConfig."""
        cfg = self.EngramConfig()
        cfg.tokenizer_name_or_path = ""
        cfg.engram_vocab_size = [1000, 1000]
        cfg.max_ngram_size = 3
        cfg.n_embed_per_ngram = 64
        cfg.n_head_per_ngram = 4
        cfg.layer_ids = [1, 15]
        cfg.pad_id = 2
        cfg.seed = 0
        cfg.kernel_size = 4

        bb = self.BackboneConfig()
        bb.hidden_size = 256
        bb.hc_mult = 4
        bb.vocab_size = 128000
        bb.num_layers = 6
        
        return cfg, bb
    
    def populate_store(self, engram, cfg):
        """Helper to populate store with random embeddings."""
        num_heads = (cfg.max_ngram_size - 1) * cfg.n_head_per_ngram
        embed_D = cfg.n_embed_per_ngram // cfg.n_head_per_ngram
        
        vocab_sizes = []
        for ngram_idx in range(cfg.max_ngram_size - 1):
            vocab_size = cfg.engram_vocab_size[ngram_idx]
            for h in range(cfg.n_head_per_ngram):
                vocab_sizes.append(vocab_size)
        
        embedding_buffers = []
        for vocab_size in vocab_sizes[:num_heads]:
            emb_table = np.random.randn(vocab_size, embed_D).astype(np.float32)
            embedding_buffers.append(emb_table)
        
        engram.populate_store_from_buffers(embedding_buffers)
        return embedding_buffers

# ============================================================================
# Test 1: Configuration and Creation
# ============================================================================

class TestEngramCreation(EngramTestBase):
    """Test Engram configuration and creation."""
    
    def test_01_config_and_creation(self):
        """Test configuration classes and basic creation."""
        # Test EngramConfig
        cfg = self.EngramConfig()
        cfg.engram_vocab_size = [1000, 1000]
        cfg.max_ngram_size = 3
        cfg.n_embed_per_ngram = 64
        cfg.n_head_per_ngram = 4
        cfg.layer_ids = [1, 15]
        cfg.pad_id = 2
        cfg.seed = 0
        cfg.kernel_size = 4
        
        # Test BackboneConfig
        bb = self.BackboneConfig()
        bb.hidden_size = 256
        bb.hc_mult = 4
        bb.vocab_size = 128000
        bb.num_layers = 6
        
        # Create Engram
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.assertIsNotNone(engram)
        print("✅ Configuration and creation test passed")
    
    def test_02_different_configs(self):
        """Test creating Engram with different configurations."""
        # Small config
        cfg1 = self.EngramConfig()
        cfg1.engram_vocab_size = [100, 100]
        cfg1.max_ngram_size = 2
        cfg1.n_embed_per_ngram = 32
        cfg1.n_head_per_ngram = 2
        cfg1.layer_ids = [1]
        cfg1.pad_id = 0
        cfg1.seed = 0
        cfg1.kernel_size = 3
        
        bb1 = self.BackboneConfig()
        bb1.hidden_size = 128
        bb1.hc_mult = 2
        bb1.vocab_size = 10000
        bb1.num_layers = 4
        
        engram1 = self.Engram(layer_id=1, config=cfg1, backbone_cfg=bb1, store=self.store)
        self.assertIsNotNone(engram1)
        
        # Large config
        cfg2 = self.EngramConfig()
        cfg2.engram_vocab_size = [5000, 5000]
        cfg2.max_ngram_size = 3
        cfg2.n_embed_per_ngram = 256
        cfg2.n_head_per_ngram = 16
        cfg2.layer_ids = [1, 5]
        cfg2.pad_id = 0
        cfg2.seed = 42
        cfg2.kernel_size = 5
        
        bb2 = self.BackboneConfig()
        bb2.hidden_size = 1024
        bb2.hc_mult = 16
        bb2.vocab_size = 200000
        bb2.num_layers = 32
        
        engram2 = self.Engram(layer_id=1, config=cfg2, backbone_cfg=bb2, store=self.store)
        self.assertIsNotNone(engram2)
        print("✅ Different configurations test passed")

# ============================================================================
# Test 2: Store Population
# ============================================================================

class TestPopulateStore(EngramTestBase):
    """Test populate_store_from_buffers method."""
    
    def test_01_populate_basic(self):
        """Test basic store population."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        
        embedding_buffers = self.populate_store(engram, cfg)
        expected_heads = (cfg.max_ngram_size - 1) * cfg.n_head_per_ngram
        self.assertEqual(len(embedding_buffers), expected_heads)
        print("✅ Store population test passed")
    
    def test_02_populate_large_tables(self):
        """Test populating with large embedding tables."""
        cfg, bb = self.create_default_configs()
        cfg.engram_vocab_size = [10000, 10000]
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        
        embedding_buffers = self.populate_store(engram, cfg)
        for buf in embedding_buffers:
            self.assertGreater(buf.size, 0)
            self.assertEqual(buf.dtype, np.float32)
        print("✅ Large embedding tables population test passed")

# ============================================================================
# Test 3: Forward Pass - Basic Cases
# ============================================================================

class TestForwardBasic(EngramTestBase):
    """Test basic forward pass functionality."""
    
    def test_01_forward_small(self):
        """Test forward with small batch and sequence."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 2, 8
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
        
        output = engram.forward(hidden_states, input_ids)
        self.assertEqual(output.shape, hidden_states.shape)
        self.assertFalse(np.any(np.isnan(output)))
        self.assertFalse(np.any(np.isinf(output)))
        print("✅ Forward pass (small) test passed")
    
    def test_02_forward_medium(self):
        """Test forward with medium batch and sequence."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 8, 32
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
        
        output = engram.forward(hidden_states, input_ids)
        self.assertEqual(output.shape, hidden_states.shape)
        self.assertFalse(np.any(np.isnan(output)))
        self.assertFalse(np.any(np.isinf(output)))
        print("✅ Forward pass (medium) test passed")
    
    def test_03_forward_large(self):
        """Test forward with large batch and long sequence."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 16, 128
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
        
        output = engram.forward(hidden_states, input_ids)
        self.assertEqual(output.shape, hidden_states.shape)
        self.assertFalse(np.any(np.isnan(output)))
        self.assertFalse(np.any(np.isinf(output)))
        print("✅ Forward pass (large) test passed")

# ============================================================================
# Test 4: Forward Pass - Edge Cases
# ============================================================================

class TestForwardEdgeCases(EngramTestBase):
    """Test forward pass edge cases."""
    
    def test_01_forward_single_token(self):
        """Test forward with single token per sequence."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 4, 1
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        input_ids = [[0], [1], [2], [3]]
        
        output = engram.forward(hidden_states, input_ids)
        self.assertEqual(output.shape, hidden_states.shape)
        self.assertFalse(np.any(np.isnan(output)))
        print("✅ Forward pass (single token) test passed")
    
    def test_02_forward_padding(self):
        """Test forward with padding tokens."""
        cfg, bb = self.create_default_configs()
        cfg.pad_id = 0
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 2, 8
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        # First sequence has padding at the end
        input_ids = [[1, 2, 3, 4, 0, 0, 0, 0], [5, 6, 7, 8, 9, 10, 11, 12]]
        
        output = engram.forward(hidden_states, input_ids)
        self.assertEqual(output.shape, hidden_states.shape)
        self.assertFalse(np.any(np.isnan(output)))
        print("✅ Forward pass (padding) test passed")
    
    def test_03_forward_large_vocab_ids(self):
        """Test forward with large vocabulary IDs."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 2, 8
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        # Use IDs near vocab_size boundary
        input_ids = [[bb.vocab_size - 1 - i for i in range(L)] for _ in range(B)]
        
        output = engram.forward(hidden_states, input_ids)
        self.assertEqual(output.shape, hidden_states.shape)
        self.assertFalse(np.any(np.isnan(output)))
        print("✅ Forward pass (large vocab IDs) test passed")

# ============================================================================
# Test 5: Memory Layout and Correctness
# ============================================================================

class TestMemoryLayout(EngramTestBase):
    """Test memory layout correctness."""
    
    def test_01_memory_layout_hc_groups(self):
        """Verify different hc groups produce different outputs."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 2, 8
        hc_mult = bb.hc_mult
        D = bb.hidden_size
        
        # Create hidden states with different values per hc group
        hidden_states = np.zeros((B, L, hc_mult, D), dtype=np.float32)
        for b in range(B):
            for l in range(L):
                for hc in range(hc_mult):
                    hidden_states[b, l, hc, :] = float(hc + 1)
        
        input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
        output = engram.forward(hidden_states, input_ids)
        
        # Verify different hc groups produce different outputs
        hc_0_output = output[:, :, 0, :]
        hc_1_output = output[:, :, 1, :]
        self.assertFalse(np.allclose(hc_0_output, hc_1_output),
                        "hc=0 and hc=1 outputs are identical (memory layout issue)")
        print("✅ Memory layout (hc groups) test passed")
    
    def test_02_memory_layout_shape_consistency(self):
        """Verify output shape always matches input shape."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        test_cases = [(1, 1), (1, 8), (2, 8), (4, 16), (8, 32), (16, 64)]
        
        for B, L in test_cases:
            hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
            input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
            
            output = engram.forward(hidden_states, input_ids)
            self.assertEqual(output.shape, hidden_states.shape,
                            f"Shape mismatch for B={B}, L={L}")
        print("✅ Memory layout (shape consistency) test passed")

# ============================================================================
# Test 6: Numerical Stability
# ============================================================================

class TestNumericalStability(EngramTestBase):
    """Test numerical stability."""
    
    def test_01_no_nan_inf(self):
        """Verify no NaN or Inf values in output."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 8, 32
        hidden_states = np.random.randn(B, L, bb.hc_mult, bb.hidden_size).astype(np.float32)
        input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
        
        output = engram.forward(hidden_states, input_ids)
        
        self.assertFalse(np.any(np.isnan(output)), "Output contains NaN values!")
        self.assertFalse(np.any(np.isinf(output)), "Output contains Inf values!")
        print("✅ Numerical stability (no NaN/Inf) test passed")
    
    def test_02_extreme_values(self):
        """Test with extreme hidden state values."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        B, L = 2, 8
        
        # Very large values
        hidden_states_large = np.full((B, L, bb.hc_mult, bb.hidden_size), 1e6, dtype=np.float32)
        input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
        output_large = engram.forward(hidden_states_large, input_ids)
        self.assertFalse(np.any(np.isnan(output_large)))
        self.assertFalse(np.any(np.isinf(output_large)))
        
        # Very small values
        hidden_states_small = np.full((B, L, bb.hc_mult, bb.hidden_size), 1e-6, dtype=np.float32)
        output_small = engram.forward(hidden_states_small, input_ids)
        self.assertFalse(np.any(np.isnan(output_small)))
        self.assertFalse(np.any(np.isinf(output_small)))
        
        # Zero values
        hidden_states_zero = np.zeros((B, L, bb.hc_mult, bb.hidden_size), dtype=np.float32)
        output_zero = engram.forward(hidden_states_zero, input_ids)
        self.assertFalse(np.any(np.isnan(output_zero)))
        self.assertFalse(np.any(np.isinf(output_zero)))
        print("✅ Numerical stability (extreme values) test passed")

# ============================================================================
# Test 7: Error Handling
# ============================================================================

class TestErrorHandling(EngramTestBase):
    """Test error handling."""
    
    def test_01_wrong_hidden_states_shape(self):
        """Test with wrong hidden_states shape."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        self.populate_store(engram, cfg)
        
        # Wrong number of dimensions
        hidden_states_3d = np.random.randn(2, 8, bb.hc_mult * bb.hidden_size).astype(np.float32)
        input_ids = [[i % bb.vocab_size for i in range(8)] for _ in range(2)]
        
        with self.assertRaises(Exception):
            engram.forward(hidden_states_3d, input_ids)
        print("✅ Error handling (wrong shape) test passed")
    
    def test_02_empty_embedding_buffers(self):
        """Test populate_store with empty buffers."""
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
        
        with self.assertRaises(Exception):
            engram.populate_store_from_buffers([])
        print("✅ Error handling (empty buffers) test passed")

# ============================================================================
# Test 8: Different Configurations
# ============================================================================

class TestDifferentConfigs(EngramTestBase):
    """Test forward pass with different configurations."""
    
    def test_01_different_hc_mult(self):
        """Test forward with different hc_mult values."""
        cfg, bb = self.create_default_configs()
        
        for hc_mult in [1, 2, 4, 8]:
            bb.hc_mult = hc_mult
            engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
            self.populate_store(engram, cfg)
            
            B, L = 2, 8
            hidden_states = np.random.randn(B, L, hc_mult, bb.hidden_size).astype(np.float32)
            input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
            
            output = engram.forward(hidden_states, input_ids)
            self.assertEqual(output.shape, (B, L, hc_mult, bb.hidden_size))
            self.assertFalse(np.any(np.isnan(output)))
        print("✅ Different hc_mult test passed")
    
    def test_02_different_hidden_size(self):
        """Test forward with different hidden_size values."""
        cfg, bb = self.create_default_configs()
        
        for hidden_size in [128, 256, 512]:
            bb.hidden_size = hidden_size
            engram = self.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=self.store)
            self.populate_store(engram, cfg)
            
            B, L = 2, 8
            hidden_states = np.random.randn(B, L, bb.hc_mult, hidden_size).astype(np.float32)
            input_ids = [[i % bb.vocab_size for i in range(L)] for _ in range(B)]
            
            output = engram.forward(hidden_states, input_ids)
            self.assertEqual(output.shape, (B, L, bb.hc_mult, hidden_size))
            self.assertFalse(np.any(np.isnan(output)))
        print("✅ Different hidden_size test passed")

if __name__ == "__main__":
    unittest.main(verbosity=2)

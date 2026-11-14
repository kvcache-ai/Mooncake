"""CPU check that the fallback's FP8 cast rounds like the native kernels.

The CUDA and XPU dispatch kernels quantize each 128-channel group with the
scale 448 / amax, a correctly rounded float32 division, and store
amax * (1 / 448) as the inverse scale. The reference below computes both in
float64 and rounds once to float32: for float32 operands that gives exactly
the correctly rounded float32 quotient and product, independently of how torch
lowers a scalar-by-tensor division.
"""

import unittest

import torch

from mooncake.mooncake_ep_buffer import Buffer


def _reference_cast(x: torch.Tensor):
    m, n = x.shape
    x_view = x.view(m, -1, 128)
    amax = x_view.abs().float().amax(dim=2).clamp(1e-4)
    scale = (448.0 / amax.double()).float()
    inv_448 = torch.tensor(1.0 / 448.0, dtype=torch.float32).double()
    scale_inv = (amax.double() * inv_448).float()
    x_fp8 = (
        (x_view.double() * scale.double().unsqueeze(2))
        .float()
        .to(torch.float8_e4m3fn)
        .view(m, n)
    )
    return x_fp8, scale_inv


class TestFallbackFp8Cast(unittest.TestCase):
    def test_matches_kernel_rounding(self):
        gen = torch.Generator().manual_seed(0)
        x = torch.randn(512, 7168, generator=gen).to(torch.bfloat16)
        x[0].zero_()  # amax clamps to 1e-4
        x[1] *= 1e-3

        x_fp8, x_scales = Buffer._fp8_cast(x)
        ref_fp8, ref_scales = _reference_cast(x)

        self.assertEqual(x_fp8.dtype, torch.float8_e4m3fn)
        self.assertEqual(tuple(x_fp8.shape), (512, 7168))
        self.assertEqual(tuple(x_scales.shape), (512, 7168 // 128))
        # Bit-for-bit: the native path's output is compared exactly.
        mismatched = (x_fp8.view(torch.uint8) != ref_fp8.view(torch.uint8)).sum()
        self.assertEqual(mismatched.item(), 0, "FP8 payload bytes differ")
        self.assertTrue(torch.equal(x_scales, ref_scales), "FP8 scales differ")


if __name__ == "__main__":
    unittest.main()

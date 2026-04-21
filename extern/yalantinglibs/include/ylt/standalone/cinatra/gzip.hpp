#pragma once
#include <zlib.h>

#include <string_view>
namespace cinatra::gzip_codec {
// from https://github.com/chafey/GZipCodec

#define CHUNK 16384
#define windowBits 15
#define GZIP_ENCODING 16

// GZip Compression
// @param data - the data to compress (does not have to be string, can be binary
// data)
// @param compressed_data - the resulting gzip compressed data
// @param level - the gzip compress level -1 = default, 0 = no compression, 1=
// worst/fastest compression, 9 = best/slowest compression
// @return - true on success, false on failure
inline bool compress(std::string_view data, std::string &compressed_data,
                     int level = -1) {
  unsigned char out[CHUNK];
  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  if (deflateInit2(&strm, level, Z_DEFLATED, windowBits | GZIP_ENCODING, 8,
                   Z_DEFAULT_STRATEGY) != Z_OK) {
    return false;
  }
  strm.next_in = (unsigned char *)data.data();
  strm.avail_in = (uInt)data.length();
  do {
    int have;
    strm.avail_out = CHUNK;
    strm.next_out = out;
    if (deflate(&strm, Z_FINISH) == Z_STREAM_ERROR) {
      return false;
    }
    have = CHUNK - strm.avail_out;
    compressed_data.append((char *)out, have);
  } while (strm.avail_out == 0);
  if (deflateEnd(&strm) != Z_OK) {
    return false;
  }
  return true;
}

// GZip Decompression
// @param compressed_data - the gzip compressed data
// @param data - the resulting uncompressed data (may contain binary data)
// @return - true on success, false on failure
inline bool uncompress(std::string_view compressed_data, std::string &data) {
  int ret;
  unsigned have;
  z_stream strm;
  unsigned char out[CHUNK];

  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = 0;
  strm.next_in = Z_NULL;
  if (inflateInit2(&strm, 16 + MAX_WBITS) != Z_OK) {
    return false;
  }

  strm.avail_in = (uInt)compressed_data.length();
  strm.next_in = (unsigned char *)compressed_data.data();
  do {
    strm.avail_out = CHUNK;
    strm.next_out = out;
    ret = inflate(&strm, Z_NO_FLUSH);
    switch (ret) {
      case Z_NEED_DICT:
      case Z_DATA_ERROR:
      case Z_MEM_ERROR:
        inflateEnd(&strm);
        return false;
    }
    have = CHUNK - strm.avail_out;
    data.append((char *)out, have);
  } while (strm.avail_out == 0);

  if (inflateEnd(&strm) != Z_OK) {
    return false;
  }

  return true;
}

inline int compress_file(const char *src_file, const char *out_file_name) {
  char buf[BUFSIZ] = {0};
  uInt bytes_read = 0;
  gzFile out = gzopen(out_file_name, "wb");
  if (!out) {
    return -1;
  }

  std::ifstream in(src_file, std::ios::binary);
  if (!in.is_open()) {
    return -1;
  }

  while (true) {
    in.read(buf, BUFSIZ);
    bytes_read = (uInt)in.gcount();
    if (bytes_read == 0)
      break;
    int bytes_written = gzwrite(out, buf, bytes_read);
    if (bytes_written == 0) {
      gzclose(out);
      return -1;
    }
    if (bytes_read != BUFSIZ)
      break;
  }
  gzclose(out);

  return 0;
}

inline int uncompress_file(const char *src_file, const char *out_file_name) {
  char buf[BUFSIZ] = {0};
  std::ofstream out(out_file_name, std::ios::binary);
  if (!out.is_open()) {
    return -1;
  }

  gzFile fi = gzopen(src_file, "rb");
  if (!fi) {
    return -1;
  }

  gzrewind(fi);
  while (!gzeof(fi)) {
    int len = gzread(fi, buf, BUFSIZ);
    out.write(buf, len);
  }
  gzclose(fi);

  return 0;
}

inline bool inflate(std::string_view str_src, std::string &str_dest) {
  int err = Z_DATA_ERROR;
  // Create stream
  z_stream zs = {0};
  // Set output data streams, do this here to avoid overwriting on recursive
  // calls
  const int OUTPUT_BUF_SIZE = 8192;
  Bytef bytes_out[OUTPUT_BUF_SIZE] = {0};

  // Initialise the z_stream
  err = ::inflateInit2(&zs, -15);
  if (err != Z_OK) {
    return false;
  }

  // Use whatever input is provided
  zs.next_in = (Bytef *)(str_src.data());
  zs.avail_in = str_src.length();

  do {
    try {
      // Initialise stream values
      // zs->zalloc = (alloc_func)0;
      // zs->zfree = (free_func)0;
      // zs->opaque = (voidpf)0;

      zs.next_out = bytes_out;
      zs.avail_out = OUTPUT_BUF_SIZE;

      // Try to unzip the data
      err = ::inflate(&zs, Z_SYNC_FLUSH);

      // Is zip finished reading all currently available input and writing all
      // generated output
      if (err == Z_STREAM_END) {
        // Finish up
        [[maybe_unused]] int kerr = ::inflateEnd(&zs);

        // Got a good result, set the size to the amount unzipped in this call
        // (including all recursive calls)

        str_dest.append((const char *)bytes_out,
                        OUTPUT_BUF_SIZE - zs.avail_out);
        return true;
      }
      else if ((err == Z_OK) && (zs.avail_out == 0) && (zs.avail_in != 0)) {
        // Output array was not big enough, call recursively until there is
        // enough space

        str_dest.append((const char *)bytes_out,
                        OUTPUT_BUF_SIZE - zs.avail_out);

        continue;
      }
      else if ((err == Z_OK) && (zs.avail_in == 0)) {
        // All available input has been processed, everything ok.
        // Set the size to the amount unzipped in this call (including all
        // recursive calls)
        str_dest.append((const char *)bytes_out,
                        OUTPUT_BUF_SIZE - zs.avail_out);

        [[maybe_unused]] int kerr = ::inflateEnd(&zs);

        break;
      }
      else {
        return false;
      }
    } catch (...) {
      return false;
    }
  } while (true);

  return err == Z_OK;
}

inline bool deflate(std::string_view str_src, std::string &str_dest) {
  int err = Z_DATA_ERROR;
  // Create stream
  z_stream zs = {0};
  // Set output data streams, do this here to avoid overwriting on recursive
  // calls
  const int OUTPUT_BUF_SIZE = 8192;
  Bytef bytes_out[OUTPUT_BUF_SIZE] = {0};

  // Initialise the z_stream
  err = ::deflateInit2(&zs, 1, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
  if (err != Z_OK) {
    return false;
  }
  // Use whatever input is provided
  zs.next_in = (Bytef *)(str_src.data());
  zs.avail_in = str_src.length();

  do {
    try {
      // Initialise stream values
      // zs->zalloc = (alloc_func)0;
      // zs->zfree = (free_func)0;
      // zs->opaque = (voidpf)0;

      zs.next_out = bytes_out;
      zs.avail_out = OUTPUT_BUF_SIZE;

      // Try to unzip the data
      err = ::deflate(&zs, Z_SYNC_FLUSH);

      // Is zip finished reading all currently available input and writing all
      // generated output
      if (err == Z_STREAM_END) {
        // Finish up
        [[maybe_unused]] int kerr = ::deflateEnd(&zs);

        // Got a good result, set the size to the amount unzipped in this call
        // (including all recursive calls)

        str_dest.append((const char *)bytes_out,
                        OUTPUT_BUF_SIZE - zs.avail_out);
        return true;
      }
      else if ((err == Z_OK) && (zs.avail_out == 0) && (zs.avail_in != 0)) {
        // Output array was not big enough, call recursively until there is
        // enough space

        str_dest.append((const char *)bytes_out,
                        OUTPUT_BUF_SIZE - zs.avail_out);

        continue;
      }
      else if ((err == Z_OK) && (zs.avail_in == 0)) {
        // All available input has been processed, everything ok.
        // Set the size to the amount unzipped in this call (including all
        // recursive calls)
        str_dest.append((const char *)bytes_out,
                        OUTPUT_BUF_SIZE - zs.avail_out);

        [[maybe_unused]] int kerr = ::deflateEnd(&zs);

        break;
      }
      else {
        return false;
      }
    } catch (...) {
      return false;
    }
  } while (true);

  if (err == Z_OK) {
    // subtract 4 to remove the extra 00 00 ff ff added to the end of the deflat
    // function
    str_dest = str_dest.substr(0, str_dest.length() - 4);
    return true;
  }

  return false;
}

}  // namespace cinatra::gzip_codec
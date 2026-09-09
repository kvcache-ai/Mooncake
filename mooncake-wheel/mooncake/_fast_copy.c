#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowArray {
    int64_t length, null_count, offset, n_buffers, n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
struct ArrowSchema {
    const char *format, *name, *metadata;
    int64_t flags, n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};
#endif

typedef struct {
    struct ArrowArray array;
    const void *data;
} PillowArrowOwner;

typedef struct {
    const void *buffers[2];
    Py_buffer view;
} BufferArrowOwner;

static void release_moved_pillow_array(PyObject *capsule) {
    PillowArrowOwner *owner =
        PyCapsule_GetPointer(capsule, "mooncake_pillow_arrow");
    if (!owner) {
        PyErr_Clear();
        return;
    }
    if (owner->array.release) owner->array.release(&owner->array);
    free(owner);
}

static int parse_pillow_arrow(PyObject *image, PillowArrowOwner **out,
                              unsigned long long *data_size) {
    PyObject *capsules = PyObject_CallMethod(image, "__arrow_c_array__", NULL);
    if (!capsules) return -1;
    if (!PyTuple_Check(capsules) || PyTuple_GET_SIZE(capsules) != 2) {
        Py_DECREF(capsules);
        PyErr_SetString(PyExc_ValueError,
                        "Pillow returned invalid Arrow capsules");
        return -1;
    }
    struct ArrowSchema *schema =
        PyCapsule_GetPointer(PyTuple_GET_ITEM(capsules, 0), "arrow_schema");
    struct ArrowArray *array =
        PyCapsule_GetPointer(PyTuple_GET_ITEM(capsules, 1), "arrow_array");
    if (!schema || !array) {
        Py_DECREF(capsules);
        return -1;
    }
    if (!schema->release || !array->release) goto invalid;
    if (!schema->format || strcmp(schema->format, "+w:4") != 0 ||
        schema->n_children != 1 || array->n_children != 1 ||
        array->n_buffers != 1 || !array->buffers || array->buffers[0] ||
        schema->dictionary || !schema->children || !array->children)
        goto invalid;
    const struct ArrowSchema *data_schema = schema->children[0];
    const struct ArrowArray *data_array = array->children[0];
    if (!data_schema || !data_array || !data_schema->release ||
        !data_array->release || !data_schema->format ||
        strcmp(data_schema->format, "C") != 0 || data_schema->n_children != 0 ||
        data_schema->dictionary || data_schema->children ||
        data_array->offset != 0 || data_array->null_count != 0 ||
        data_array->n_buffers != 2 || data_array->n_children != 0 ||
        data_array->dictionary || data_array->children ||
        !data_array->buffers || data_array->buffers[0] ||
        !data_array->buffers[1] || array->offset != 0 ||
        array->null_count != 0 || array->dictionary || array->length < 0 ||
        array->length > INT64_MAX / 4 ||
        data_array->length != array->length * 4)
        goto invalid;
    PillowArrowOwner *owner = calloc(1, sizeof(*owner));
    if (!owner) {
        Py_DECREF(capsules);
        PyErr_NoMemory();
        return -1;
    }
    owner->array = *array;
    array->release = NULL;
    *data_size = (unsigned long long)data_array->length;
    owner->data = data_array->buffers[1];
    Py_DECREF(capsules);
    *out = owner;
    return 0;
invalid:
    Py_DECREF(capsules);
    PyErr_SetString(PyExc_ValueError, "unsupported Pillow Arrow layout");
    return -1;
}

static PyObject *pillow_arrow_view(PyObject *self, PyObject *image) {
    PillowArrowOwner *owner = NULL;
    unsigned long long size = 0;
    if (parse_pillow_arrow(image, &owner, &size) < 0) return NULL;
    if (size > NPY_MAX_INTP) {
        if (owner->array.release) owner->array.release(&owner->array);
        free(owner);
        return PyErr_Format(PyExc_OverflowError,
                            "Pillow Arrow buffer is too large: %llu", size);
    }
    npy_intp dims[1] = {(npy_intp)size};
    PyObject *array =
        PyArray_SimpleNewFromData(1, dims, NPY_UINT8, (void *)owner->data);
    if (!array) {
        if (owner->array.release) owner->array.release(&owner->array);
        free(owner);
        return NULL;
    }
    PyObject *capsule = PyCapsule_New(owner, "mooncake_pillow_arrow",
                                      release_moved_pillow_array);
    if (!capsule) {
        Py_DECREF(array);
        if (owner->array.release) owner->array.release(&owner->array);
        free(owner);
        return NULL;
    }
    if (PyArray_SetBaseObject((PyArrayObject *)array, capsule) < 0) {
        Py_DECREF(array);
        return NULL;
    }
    PyArray_CLEARFLAGS((PyArrayObject *)array, NPY_ARRAY_WRITEABLE);
    return array;
}

static void release_buffer_arrow(struct ArrowArray *array) {
    if (!array || !array->release) return;
    BufferArrowOwner *owner = array->private_data;
    array->release = NULL;
    PyGILState_STATE state = PyGILState_Ensure();
    PyBuffer_Release(&owner->view);
    PyGILState_Release(state);
    free(owner);
}

static void release_arrow_schema(struct ArrowSchema *schema) {
    if (!schema || !schema->release) return;
    schema->release = NULL;
}

static void destroy_arrow_array_capsule(PyObject *capsule) {
    struct ArrowArray *array = PyCapsule_GetPointer(capsule, "arrow_array");
    if (!array) {
        PyErr_Clear();
        return;
    }
    if (array->release) array->release(array);
    free(array);
}

static void destroy_arrow_schema_capsule(PyObject *capsule) {
    struct ArrowSchema *schema = PyCapsule_GetPointer(capsule, "arrow_schema");
    if (!schema) {
        PyErr_Clear();
        return;
    }
    if (schema->release) schema->release(schema);
    free(schema);
}

static PyObject *export_arrow_u8(PyObject *self, PyObject *args) {
    PyObject *buffer;
    PyObject *requested = Py_None;
    if (!PyArg_ParseTuple(args, "O|O", &buffer, &requested)) return NULL;
    (void)requested;
    BufferArrowOwner *owner = calloc(1, sizeof(*owner));
    struct ArrowArray *array = calloc(1, sizeof(*array));
    struct ArrowSchema *schema = calloc(1, sizeof(*schema));
    if (!owner || !array || !schema) {
        free(owner);
        free(array);
        free(schema);
        return PyErr_NoMemory();
    }
    if (PyObject_GetBuffer(buffer, &owner->view, PyBUF_CONTIG_RO) < 0) {
        free(owner);
        free(array);
        free(schema);
        return NULL;
    }
    if (owner->view.len < 0 || (owner->view.len > 0 && !owner->view.buf)) {
        PyBuffer_Release(&owner->view);
        free(owner);
        free(array);
        free(schema);
        PyErr_SetString(PyExc_ValueError, "invalid Arrow buffer");
        return NULL;
    }
    owner->buffers[1] = owner->view.buf;
    array->length = owner->view.len;
    array->null_count = 0;
    array->n_buffers = 2;
    array->buffers = owner->buffers;
    array->release = release_buffer_arrow;
    array->private_data = owner;
    schema->format = "C";
    schema->release = release_arrow_schema;
    PyObject *schema_capsule =
        PyCapsule_New(schema, "arrow_schema", destroy_arrow_schema_capsule);
    if (!schema_capsule) {
        array->release(array);
        free(array);
        free(schema);
        return NULL;
    }
    PyObject *array_capsule =
        PyCapsule_New(array, "arrow_array", destroy_arrow_array_capsule);
    if (!array_capsule) {
        Py_DECREF(schema_capsule);
        array->release(array);
        free(array);
        return NULL;
    }
    PyObject *result = PyTuple_Pack(2, schema_capsule, array_capsule);
    Py_DECREF(schema_capsule);
    Py_DECREF(array_capsule);
    return result;
}

typedef struct {
    void **src_ptrs;
    size_t *src_sizes;
    int count;
    char *dst;
    size_t offset;
    size_t bytes_copied;
} ThreadWork;

static void *copy_thread_func(void *arg) {
    ThreadWork *w = (ThreadWork *)arg;
    char *d = w->dst + w->offset;
    size_t off = 0;
    for (int i = 0; i < w->count; i++) {
        if (w->src_sizes[i] > 0) {
            memcpy(d + off, w->src_ptrs[i], w->src_sizes[i]);
            off += w->src_sizes[i];
        }
    }
    w->bytes_copied = off;
    return NULL;
}

static PyObject *concat_arrays_into(PyObject *self, PyObject *args) {
    PyObject *list_obj;
    unsigned long long dest_ptr_val;
    unsigned long long dest_size_val;
    Py_ssize_t start = 0;
    Py_ssize_t count = -1;
    int nthreads = 1;
    ThreadWork *works = NULL;
    pthread_t *threads = NULL;
    char *thread_started = NULL;

    if (!PyArg_ParseTuple(args, "O!KK|nni", &PyList_Type, &list_obj,
                          &dest_ptr_val, &dest_size_val, &start, &count,
                          &nthreads))
        return NULL;

    Py_ssize_t list_len = PyList_GET_SIZE(list_obj);
    if (start < 0) start = 0;
    if (start > list_len) start = list_len;
    if (count < 0 || start + count > list_len) count = list_len - start;
    if (count == 0) return PyLong_FromSize_t(0);
    if (nthreads < 1) nthreads = 1;
    if (nthreads > (int)count) nthreads = (int)count;

    size_t dest_size = (size_t)dest_size_val;
    void **ptrs = (void **)malloc(count * sizeof(void *));
    size_t *sizes = (size_t *)malloc(count * sizeof(size_t));
    PyObject **items = (PyObject **)calloc(count, sizeof(PyObject *));
    if (!ptrs || !sizes || !items) {
        free(ptrs);
        free(sizes);
        free(items);
        return PyErr_NoMemory();
    }

    Py_ssize_t held = 0;
    size_t total_input_bytes = 0;
    for (Py_ssize_t i = 0; i < count; i++) {
        PyObject *item = PyList_GET_ITEM(list_obj, start + i);
        if (!PyArray_Check(item)) {
            PyErr_Format(PyExc_TypeError, "arrays[%zd] is not an ndarray",
                         start + i);
            goto fail;
        }
        Py_INCREF(item);
        items[held++] = item;
        PyArrayObject *arr = (PyArrayObject *)item;
        if (!PyArray_IS_C_CONTIGUOUS(arr)) {
            PyErr_Format(PyExc_ValueError, "arrays[%zd] is not C-contiguous",
                         start + i);
            goto fail;
        }
        size_t nbytes = (size_t)PyArray_NBYTES(arr);
        if (nbytes > SIZE_MAX - total_input_bytes) {
            PyErr_SetString(PyExc_OverflowError, "array byte sizes overflow");
            goto fail;
        }
        ptrs[i] = PyArray_DATA(arr);
        sizes[i] = nbytes;
        total_input_bytes += nbytes;
    }
    if (total_input_bytes > dest_size) {
        PyErr_Format(PyExc_ValueError,
                     "destination buffer too small: need %zu bytes, got %zu",
                     total_input_bytes, dest_size);
        goto fail;
    }

    /* Partition work across threads. */
    works = (ThreadWork *)calloc(nthreads, sizeof(ThreadWork));
    threads = (pthread_t *)malloc(nthreads * sizeof(pthread_t));
    thread_started = (char *)calloc(nthreads, sizeof(char));
    if (!works || !threads || !thread_started) {
        PyErr_NoMemory();
        goto fail;
    }

    int n = (int)count;
    int per_t = (n + nthreads - 1) / nthreads;
    size_t offset = 0;
    int actual_threads = 0;
    for (int t = 0; t < nthreads; t++) {
        int s = t * per_t;
        int c = per_t;
        if (s + c > n) c = n - s;
        if (c <= 0) break;

        works[t].src_ptrs = ptrs + s;
        works[t].src_sizes = sizes + s;
        works[t].count = c;
        works[t].dst = (char *)(uintptr_t)dest_ptr_val;
        works[t].offset = offset;

        size_t tb = 0;
        for (int j = s; j < s + c; j++) tb += sizes[j];
        offset += tb;
        actual_threads++;
    }

    Py_BEGIN_ALLOW_THREADS;
    if (actual_threads == 1) {
        copy_thread_func(&works[0]);
    } else {
        for (int t = 1; t < actual_threads; t++) {
            if (pthread_create(&threads[t], NULL, copy_thread_func,
                               &works[t]) != 0) {
                /* Copy inline if worker creation fails after other workers
                 * start. */
                copy_thread_func(&works[t]);
            } else {
                thread_started[t] = 1;
            }
        }
        copy_thread_func(&works[0]);
        for (int t = 1; t < actual_threads; t++) {
            if (thread_started[t]) {
                pthread_join(threads[t], NULL);
            }
        }
    }
    Py_END_ALLOW_THREADS;

    size_t total = 0;
    for (int t = 0; t < actual_threads; t++) total += works[t].bytes_copied;

    free(ptrs);
    free(sizes);
    for (Py_ssize_t i = 0; i < held; i++) Py_DECREF(items[i]);
    free(items);
    free(works);
    free(threads);
    free(thread_started);
    return PyLong_FromSize_t(total);

fail:
    free(ptrs);
    free(sizes);
    for (Py_ssize_t i = 0; i < held; i++) Py_DECREF(items[i]);
    free(items);
    free(works);
    free(threads);
    free(thread_started);
    return NULL;
}

static PyMethodDef module_methods[] = {
    {"concat_arrays_into", concat_arrays_into, METH_VARARGS,
     "Scatter-copy arrays[start:start+count] into dest_ptr (GIL released)."},
    {"pillow_arrow_view", pillow_arrow_view, METH_O,
     "Return a read-only uint8 view over Pillow Arrow pixel storage."},
    {"export_arrow_u8", export_arrow_u8, METH_VARARGS,
     "Export a contiguous buffer through the Arrow PyCapsule interface."},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_fast_copy",
    "Fast scatter-gather copy for ndarray lists.",
    -1,
    module_methods,
};

PyMODINIT_FUNC PyInit__fast_copy(void) {
    import_array();
    return PyModule_Create(&moduledef);
}

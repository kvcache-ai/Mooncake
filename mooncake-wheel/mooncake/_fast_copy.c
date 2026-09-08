#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>

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

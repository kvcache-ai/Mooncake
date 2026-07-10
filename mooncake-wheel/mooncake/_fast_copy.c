/*
 * _fast_copy.c — Python C extension for scatter-gather copy with GIL release.
 *
 * concat_arrays_into(arrays, dest_ptr, start=0, count=-1, nthreads=1)
 *   Copies arrays[start:start+count] into dest_ptr using nthreads.
 *   Releases GIL during the memcpy phase, enabling concurrent copies
 *   from multiple Python threads.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
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

static PyObject *
concat_arrays_into(PyObject *self, PyObject *args)
{
    PyObject *list_obj;
    unsigned long long dest_ptr_val;
    Py_ssize_t start = 0;
    Py_ssize_t count = -1;
    int nthreads = 1;

    if (!PyArg_ParseTuple(args, "O!K|nni",
                          &PyList_Type, &list_obj,
                          &dest_ptr_val,
                          &start, &count, &nthreads))
        return NULL;

    Py_ssize_t list_len = PyList_GET_SIZE(list_obj);
    if (start < 0) start = 0;
    if (start > list_len) start = list_len;
    if (count < 0 || start + count > list_len)
        count = list_len - start;
    if (count == 0) return PyLong_FromSize_t(0);
    if (nthreads < 1) nthreads = 1;
    if (nthreads > (int)count) nthreads = (int)count;

    /* Extract array pointers under GIL. */
    void **ptrs = (void **)malloc(count * sizeof(void *));
    size_t *sizes = (size_t *)malloc(count * sizeof(size_t));
    if (!ptrs || !sizes) {
        free(ptrs); free(sizes);
        return PyErr_NoMemory();
    }

    for (Py_ssize_t i = 0; i < count; i++) {
        PyObject *item = PyList_GET_ITEM(list_obj, start + i);
        if (!PyArray_Check(item)) {
            free(ptrs); free(sizes);
            PyErr_Format(PyExc_TypeError,
                         "arrays[%zd] is not an ndarray", start + i);
            return NULL;
        }
        PyArrayObject *arr = (PyArrayObject *)item;
        if (!PyArray_IS_C_CONTIGUOUS(arr)) {
            free(ptrs); free(sizes);
            PyErr_Format(PyExc_ValueError,
                         "arrays[%zd] is not C-contiguous", start + i);
            return NULL;
        }
        ptrs[i] = PyArray_DATA(arr);
        sizes[i] = (size_t)PyArray_NBYTES(arr);
    }

    /* Partition work across threads. */
    ThreadWork *works = (ThreadWork *)calloc(nthreads, sizeof(ThreadWork));
    pthread_t *threads = (pthread_t *)malloc(nthreads * sizeof(pthread_t));
    if (!works || !threads) {
        free(ptrs); free(sizes); free(works); free(threads);
        return PyErr_NoMemory();
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

    /* Copy with GIL released. */
    Py_BEGIN_ALLOW_THREADS
    if (actual_threads == 1) {
        copy_thread_func(&works[0]);
    } else {
        int launched = 0;
        for (int t = 1; t < actual_threads; t++) {
            if (pthread_create(&threads[t], NULL, copy_thread_func, &works[t]) != 0) {
                copy_thread_func(&works[t]);
            } else {
                launched++;
            }
        }
        copy_thread_func(&works[0]);
        for (int t = 1; t < actual_threads; t++) {
            if (launched > 0) {
                pthread_join(threads[t], NULL);
                launched--;
            }
        }
    }
    Py_END_ALLOW_THREADS

    size_t total = 0;
    for (int t = 0; t < actual_threads; t++)
        total += works[t].bytes_copied;

    free(ptrs); free(sizes); free(works); free(threads);
    return PyLong_FromSize_t(total);
}

static PyMethodDef module_methods[] = {
    {"concat_arrays_into", concat_arrays_into, METH_VARARGS,
     "Scatter-copy arrays[start:start+count] into dest_ptr (GIL released)."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_fast_copy",
    "Fast scatter-gather copy for ndarray lists.",
    -1,
    module_methods,
};

PyMODINIT_FUNC
PyInit__fast_copy(void)
{
    import_array();
    return PyModule_Create(&moduledef);
}

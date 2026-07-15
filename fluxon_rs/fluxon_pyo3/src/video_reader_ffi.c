#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/error.h>
#include <libavutil/imgutils.h>
#include <libswscale/swscale.h>

typedef int (*FluxonVideoReadAtFn)(
    void *user_data,
    int64_t offset,
    uint8_t *buf,
    int buf_size,
    int *out_len,
    char *err_buf,
    size_t err_buf_len);

typedef struct FluxonVideoIo {
    void *user_data;
    FluxonVideoReadAtFn read_at;
} FluxonVideoIo;

typedef struct FluxonAvioOpaque {
    const FluxonVideoIo *io;
    int64_t pos;
    int64_t size;
    char *err_buf;
    size_t err_buf_len;
} FluxonAvioOpaque;

static void fluxon_set_err(char *err_buf, size_t err_buf_len, const char *msg) {
    if (err_buf == NULL || err_buf_len == 0) {
        return;
    }
    snprintf(err_buf, err_buf_len, "%s", msg);
}

static void fluxon_set_av_err(char *err_buf, size_t err_buf_len, const char *ctx, int err) {
    char av_msg[AV_ERROR_MAX_STRING_SIZE] = {0};
    av_strerror(err, av_msg, sizeof(av_msg));
    if (err_buf == NULL || err_buf_len == 0) {
        return;
    }
    snprintf(err_buf, err_buf_len, "%s: %s", ctx, av_msg);
}

static int fluxon_avio_read(void *opaque, uint8_t *buf, int buf_size) {
    FluxonAvioOpaque *state = (FluxonAvioOpaque *)opaque;
    if (state == NULL || state->io == NULL || state->io->read_at == NULL) {
        return AVERROR(EIO);
    }
    if (buf_size <= 0) {
        return 0;
    }
    if (state->pos >= state->size) {
        return AVERROR_EOF;
    }

    int want = buf_size;
    int64_t available = state->size - state->pos;
    if (available < want) {
        want = (int)available;
    }
    if (want <= 0) {
        return AVERROR_EOF;
    }

    int out_len = 0;
    int ok = state->io->read_at(
        state->io->user_data,
        state->pos,
        buf,
        want,
        &out_len,
        state->err_buf,
        state->err_buf_len);
    if (ok != 0) {
        return AVERROR(EIO);
    }
    if (out_len <= 0) {
        return AVERROR_EOF;
    }
    state->pos += out_len;
    return out_len;
}

static int64_t fluxon_avio_seek(void *opaque, int64_t offset, int whence) {
    FluxonAvioOpaque *state = (FluxonAvioOpaque *)opaque;
    if (state == NULL) {
        return AVERROR(EIO);
    }
    if (whence == AVSEEK_SIZE) {
        return state->size;
    }

    int whence_base = whence & ~AVSEEK_FORCE;
    int64_t new_pos = 0;
    if (whence_base == SEEK_SET) {
        new_pos = offset;
    } else if (whence_base == SEEK_CUR) {
        new_pos = state->pos + offset;
    } else if (whence_base == SEEK_END) {
        new_pos = state->size + offset;
    } else {
        return AVERROR(EINVAL);
    }
    if (new_pos < 0) {
        return AVERROR(EINVAL);
    }
    state->pos = new_pos;
    return new_pos;
}

static int fluxon_decode_receive_frames(
    AVCodecContext *codec_ctx,
    AVFrame *frame,
    struct SwsContext **sws_ctx,
    const int64_t *indices,
    int indices_len,
    uint8_t *filled,
    int *filled_count,
    int64_t *frame_index,
    int out_width,
    int out_height,
    uint8_t *out_data,
    int64_t frame_bytes,
    char *err_buf,
    size_t err_buf_len) {
    while (1) {
        int ret = avcodec_receive_frame(codec_ctx, frame);
        if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
            return 0;
        }
        if (ret < 0) {
            fluxon_set_av_err(err_buf, err_buf_len, "avcodec_receive_frame failed", ret);
            return -1;
        }

        for (int i = 0; i < indices_len; i++) {
            if (filled[i] || indices[i] != *frame_index) {
                continue;
            }
            *sws_ctx = sws_getCachedContext(
                *sws_ctx,
                frame->width,
                frame->height,
                (enum AVPixelFormat)frame->format,
                out_width,
                out_height,
                AV_PIX_FMT_RGB24,
                SWS_BILINEAR,
                NULL,
                NULL,
                NULL);
            if (*sws_ctx == NULL) {
                fluxon_set_err(err_buf, err_buf_len, "sws_getCachedContext failed");
                return -1;
            }

            uint8_t *dst_data[4] = {
                out_data + ((int64_t)i * frame_bytes),
                NULL,
                NULL,
                NULL,
            };
            int dst_linesize[4] = {out_width * 3, 0, 0, 0};
            int scaled = sws_scale(
                *sws_ctx,
                (const uint8_t *const *)frame->data,
                frame->linesize,
                0,
                frame->height,
                dst_data,
                dst_linesize);
            if (scaled != out_height) {
                fluxon_set_err(err_buf, err_buf_len, "sws_scale produced incomplete frame");
                return -1;
            }
            filled[i] = 1;
            *filled_count += 1;
        }

        *frame_index += 1;
        av_frame_unref(frame);
        if (*filled_count >= indices_len) {
            return 1;
        }
    }
}

int fluxon_fs_video_decode_frames(
    const FluxonVideoIo *io,
    int64_t file_size,
    const int64_t *indices,
    int indices_len,
    int out_width,
    int out_height,
    int num_threads,
    uint8_t *out_data,
    int64_t out_data_len,
    char *err_buf,
    size_t err_buf_len) {
    if (io == NULL || io->read_at == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "missing FluxonVideoIo read callback");
        return -1;
    }
    if (file_size < 0 || indices_len < 0 || out_width <= 0 || out_height <= 0 || num_threads <= 0) {
        fluxon_set_err(err_buf, err_buf_len, "invalid video decode argument");
        return -1;
    }
    if (indices_len == 0) {
        return 0;
    }
    for (int i = 0; i < indices_len; i++) {
        if (indices[i] < 0) {
            fluxon_set_err(err_buf, err_buf_len, "frame index must be non-negative");
            return -1;
        }
    }

    int64_t frame_bytes = (int64_t)out_width * (int64_t)out_height * 3;
    if (frame_bytes <= 0 || out_data_len != frame_bytes * (int64_t)indices_len) {
        fluxon_set_err(err_buf, err_buf_len, "output buffer size mismatch");
        return -1;
    }

    int ret = 0;
    const int avio_buffer_size = 64 * 1024;
    uint8_t *avio_buffer = NULL;
    AVIOContext *avio_ctx = NULL;
    AVFormatContext *fmt_ctx = NULL;
    AVCodecContext *codec_ctx = NULL;
#if LIBAVFORMAT_VERSION_MAJOR < 59
    AVCodec *decoder = NULL;
#else
    const AVCodec *decoder = NULL;
#endif
    AVPacket *packet = NULL;
    AVFrame *frame = NULL;
    struct SwsContext *sws_ctx = NULL;
    uint8_t *filled = NULL;

    FluxonAvioOpaque opaque = {
        .io = io,
        .pos = 0,
        .size = file_size,
        .err_buf = err_buf,
        .err_buf_len = err_buf_len,
    };

    avio_buffer = av_malloc(avio_buffer_size);
    if (avio_buffer == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "av_malloc AVIO buffer failed");
        ret = -1;
        goto cleanup;
    }
    avio_ctx = avio_alloc_context(
        avio_buffer,
        avio_buffer_size,
        0,
        &opaque,
        fluxon_avio_read,
        NULL,
        fluxon_avio_seek);
    if (avio_ctx == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "avio_alloc_context failed");
        ret = -1;
        goto cleanup;
    }
    avio_buffer = NULL;

    fmt_ctx = avformat_alloc_context();
    if (fmt_ctx == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "avformat_alloc_context failed");
        ret = -1;
        goto cleanup;
    }
    fmt_ctx->pb = avio_ctx;
    fmt_ctx->flags |= AVFMT_FLAG_CUSTOM_IO;

    int open_ret = avformat_open_input(&fmt_ctx, NULL, NULL, NULL);
    if (open_ret < 0) {
        fluxon_set_av_err(err_buf, err_buf_len, "avformat_open_input failed", open_ret);
        ret = -1;
        goto cleanup;
    }

    int info_ret = avformat_find_stream_info(fmt_ctx, NULL);
    if (info_ret < 0) {
        fluxon_set_av_err(err_buf, err_buf_len, "avformat_find_stream_info failed", info_ret);
        ret = -1;
        goto cleanup;
    }

    int stream_idx = av_find_best_stream(fmt_ctx, AVMEDIA_TYPE_VIDEO, -1, -1, &decoder, 0);
    if (stream_idx < 0) {
        fluxon_set_av_err(err_buf, err_buf_len, "av_find_best_stream failed", stream_idx);
        ret = -1;
        goto cleanup;
    }
    AVStream *stream = fmt_ctx->streams[stream_idx];
    if (decoder == NULL) {
        decoder = avcodec_find_decoder(stream->codecpar->codec_id);
    }
    if (decoder == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "missing video decoder");
        ret = -1;
        goto cleanup;
    }

    codec_ctx = avcodec_alloc_context3(decoder);
    if (codec_ctx == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "avcodec_alloc_context3 failed");
        ret = -1;
        goto cleanup;
    }
    int params_ret = avcodec_parameters_to_context(codec_ctx, stream->codecpar);
    if (params_ret < 0) {
        fluxon_set_av_err(err_buf, err_buf_len, "avcodec_parameters_to_context failed", params_ret);
        ret = -1;
        goto cleanup;
    }
    codec_ctx->thread_count = num_threads;
    int codec_ret = avcodec_open2(codec_ctx, decoder, NULL);
    if (codec_ret < 0) {
        fluxon_set_av_err(err_buf, err_buf_len, "avcodec_open2 failed", codec_ret);
        ret = -1;
        goto cleanup;
    }

    packet = av_packet_alloc();
    frame = av_frame_alloc();
    filled = av_mallocz(indices_len);
    if (packet == NULL || frame == NULL || filled == NULL) {
        fluxon_set_err(err_buf, err_buf_len, "packet/frame allocation failed");
        ret = -1;
        goto cleanup;
    }

    int filled_count = 0;
    int64_t frame_index = 0;
    while (filled_count < indices_len) {
        int read_ret = av_read_frame(fmt_ctx, packet);
        if (read_ret == AVERROR_EOF) {
            break;
        }
        if (read_ret < 0) {
            fluxon_set_av_err(err_buf, err_buf_len, "av_read_frame failed", read_ret);
            ret = -1;
            goto cleanup;
        }

        if (packet->stream_index == stream_idx) {
            int send_ret = avcodec_send_packet(codec_ctx, packet);
            if (send_ret < 0) {
                av_packet_unref(packet);
                fluxon_set_av_err(err_buf, err_buf_len, "avcodec_send_packet failed", send_ret);
                ret = -1;
                goto cleanup;
            }
            int recv = fluxon_decode_receive_frames(
                codec_ctx,
                frame,
                &sws_ctx,
                indices,
                indices_len,
                filled,
                &filled_count,
                &frame_index,
                out_width,
                out_height,
                out_data,
                frame_bytes,
                err_buf,
                err_buf_len);
            if (recv < 0) {
                av_packet_unref(packet);
                ret = -1;
                goto cleanup;
            }
        }
        av_packet_unref(packet);
    }

    if (filled_count < indices_len) {
        int flush_ret = avcodec_send_packet(codec_ctx, NULL);
        if (flush_ret < 0) {
            fluxon_set_av_err(err_buf, err_buf_len, "avcodec_send_packet flush failed", flush_ret);
            ret = -1;
            goto cleanup;
        }
        int recv = fluxon_decode_receive_frames(
            codec_ctx,
            frame,
            &sws_ctx,
            indices,
            indices_len,
            filled,
            &filled_count,
            &frame_index,
            out_width,
            out_height,
            out_data,
            frame_bytes,
            err_buf,
            err_buf_len);
        if (recv < 0) {
            ret = -1;
            goto cleanup;
        }
    }

    if (filled_count < indices_len) {
        fluxon_set_err(err_buf, err_buf_len, "requested frame index is beyond decoded video");
        ret = -1;
        goto cleanup;
    }

cleanup:
    if (sws_ctx != NULL) {
        sws_freeContext(sws_ctx);
    }
    if (frame != NULL) {
        av_frame_free(&frame);
    }
    if (packet != NULL) {
        av_packet_free(&packet);
    }
    if (codec_ctx != NULL) {
        avcodec_free_context(&codec_ctx);
    }
    if (fmt_ctx != NULL) {
        avformat_close_input(&fmt_ctx);
    }
    if (avio_ctx != NULL) {
        avio_context_free(&avio_ctx);
    }
    if (avio_buffer != NULL) {
        av_free(avio_buffer);
    }
    if (filled != NULL) {
        av_free(filled);
    }
    return ret;
}

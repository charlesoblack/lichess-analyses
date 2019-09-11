#! /usr/bin/env python

import sys
import warnings
import multiprocessing
import chess
import chess.pgn
import queue
from io import StringIO
import stockfish
import re
import pandas as pd
import time


def queue_fill(q, games_count):
    game = []
    after_moves = False
    after_pgn = False

    with open('F:/lichess/august_games.pgn', 'r') as f:
        for line in f:
            game.append(line)
            if line == '\n':
                if after_moves:
                    after_pgn = True
                    after_moves = False
                else:
                    after_moves = True
            if after_pgn:
                parsed_game = chess.pgn.read_game(StringIO(''.join(game)))
                game = []
                after_pgn = False
                with games_count.get_lock():
                    games_count.value += 1
                q.put(parsed_game)
                if q.qsize() > 10000:
                    time.sleep(1)
    return


def parse_pgn(q, infos_q):
    sf = stockfish.Stockfish('./stockfish-10-win/Windows/stockfish_10_x64.exe',
                             depth=8)
    while True:
        try:
            parsed_game = q.get(timeout=5)
        except queue.Empty:
            print("    Queue empty!    ")
            return

        fen = parsed_game.end().board().fen()
        end = parsed_game.headers['Termination']
        game_link = parsed_game.headers['Site'][20:]
        black = parsed_game.headers['Black']
        white = parsed_game.headers['White']
        time_control = parsed_game.headers['TimeControl']
        white_elo = int(parsed_game.headers['WhiteElo'])
        black_elo = int(parsed_game.headers['BlackElo'])
        result = parsed_game.headers['Result']
        
        sf.set_fen_position(fen)
        sf.get_best_move()
        info_string = sf.info
        
        rating_match = re.search(r'score (cp|mate) (.+?)(?: |$)',info_string)
        if rating_match.group(1) == 'mate':
            original_rating = int(rating_match.group(2))
            if original_rating:
                rating = 9999 * original_rating / abs(original_rating)
            elif parsed_game.headers['Result'] == '1-0':
                rating = 9999
            else:
                rating = -9999
        else:
            rating = int(rating_match.group(2))
        if ' b ' in fen:
            rating *= -1
        infos_q.put([game_link,
                     white,
                     black,
                     rating,
                     end,
                     time_control,
                     white_elo,
                     black_elo,
                     result])


def write_infos(infos_q, games_count):
    with open('F:/lichess/parsed_infos.csv', 'a') as f:
        f.write(','.join(['game_link',
                          'white',
                          'black',
                          'rating',
                          'end',
                          'time_control',
                          'white_elo',
                          'black_elo',
                          'result']) + '\n')
        while written < games_count:
            try:
                parsed_infos = infos_q.get(timeout=5)
            except queue.Empty:
                print("    Infos queue empty!    ")
                time.sleep(1)
                continue

            f.write(','.join(parsed_infos) + '\n')
            written += 1
            if written % 10000 == 0:
                print('\rParsed {}/{}'.format(written, games_count))
    return


if __name__ == '__main__':

    manager = multiprocessing.Manager()
    games_count = manager.Value(int, 0)
    q = multiprocessing.Queue()
    infos_q = multiprocessing.Queue()

    p1 = multiprocessing.Process(target=queue_fill,
                                 args=(q, games_count))

    p2 = multiprocessing.Process(target=write_infos,
                                 args=(infos_q, games_count))
    
    procs = {}

    for p in range(0, 10):
        procs[p] = multiprocessing.Process(target=parse_pgn,
                                           args=(q, infos_q))

    p1.start()

    for p in procs.values():
        p.start()

    p2.start()

    p1.join()

    for p in procs.values():
        p.join()

    p2.join()

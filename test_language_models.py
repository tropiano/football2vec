"""
Test the data object builder
"""

from datetime import datetime
import numpy as np
from lib.models import build_language_models
from lib.models import Action2Vec
from lib.data_processing import build_data_objects
from lib.data_handler_revpool import load_all_events_data as load_all_events_data_revpool
import pandas as pd

save_events_data = True
test_small_data = True
force_create = True  # Whether to force override all artifacts, or to try load existing artifacts
verbose = True  # Prints control
plotly_export = False  # Whether to export Plotly figures to Plotly studio (see https://chart-studio.plotly.com)
save_artifacts = True  # Whether to save the artifacts in to params.PATHS.ARTIFACTS

if __name__ == '__main__':
    t0 = datetime.now()
    '''
    build_data_objects(verbose=verbose,
                       force_create=force_create,
                       plotly_export=plotly_export,
                       save_artifacts=save_artifacts)'''

    t1 = datetime.now()

    # load revpool events data
    # test on small dataset
    if test_small_data:
        # events_data = pd.read_csv("../../data/test_revpool.csv")
        events_data = pd.read_csv("../../data/test_statsbomb.csv")
    else:
        data_file = "../../data/belgium-1.tar.gz"
        events_data = load_all_events_data_revpool(verbose=True, data_file=data_file)
        # save intermediate data
        if save_events_data:
            events_data.to_csv("../../data/belgium-1.csv", index=False)
    
    # train Action2Vec    
    action_2_vec, actions_vocab_data, actions_corpus, actions_embeddings = Action2Vec(events_data,
                                                                            force_create=force_create,
                                                                            plotly_export=plotly_export,
                                                                            save_artifacts=save_artifacts,
                                                                            verbose=verbose)
    t_end = datetime.now()

    print('Total run time:', np.round((t_end-t0).seconds/60, 2), 'minutes')
    print('Total run time for build_data_objects:', np.round((t1-t0).seconds/60, 2), 'minutes')
    print('Total run time for build_language_models:', np.round((t_end-t1).seconds/60, 2), 'minutes')

    import platform
    print('Machine specs:')
    print('- Machine:', platform.machine())
    print('- Machine version', platform.version())
    print('- Machine platform', platform.platform())
    print('- Machine uname', platform.uname())
    print('- Machine system', platform.system())
    print('- Machine processor', platform.processor())

    '''
    models_outputs = {'actions_vocab_data': actions_vocab_data,
                      'actions_corpus': actions_corpus,
                      # 'players_corpus': players_corpus,
                      'actions_embeddings': actions_embeddings,
                      #'players_embeddings': players_embeddings,
                      #'players_matches_embeddings': players_matches_embeddings
                      }

    return action_2_vec, player_2_vec_model, models_outputs
    # return action_2_vec, player_2_vec_model, models_outputs
    '''